#!/usr/bin/env python3
"""
Homegames LLM model server.

A pure compute worker, driven by the Node parent (../index.js). It loads a
local MLX model once (kept warm) and answers "modify my game" generation
requests over a line-delimited JSON protocol on stdin/stdout:

    stdin  (one JSON object per line):
        {"id": "<requestId>", "source": "<current index.js>", "prompt": "<edit request>",
         "mode": "CREATE"?}  # mode CREATE = generate a new game from the starter template

    stdout (one JSON object per line):
        {"id": "...", "status": "COMPLETED", "result": "<new index.js>"}
        {"id": "...", "status": "FAILED",    "error": "<message>"}

    Plus exactly one readiness line, emitted once the model is loaded:
        {"ready": true}

CHANNEL DISCIPLINE: stdout carries protocol JSON ONLY. Every diagnostic goes to
stderr (see log()), so model-loading chatter and per-attempt logs can never
corrupt the protocol stream the parent is parsing.

AMQP, the Homegames API call, and the worker secret are owned by the Node
parent — this process does not import pika or requests.
"""

import functools
import json
import os
import subprocess
import sys
import tempfile

import config
from prompts import build_messages, extract_code, system_prompt

# All human-readable output goes to stderr; stdout is reserved for protocol.
log = functools.partial(print, file=sys.stderr, flush=True)


# Lazy-loaded model handles (loading is slow; do it once at startup).
_model = None
_tokenizer = None
_draft_model = None       # optional speculative-decoding draft model
_draft_ok = True          # flipped off if the installed mlx_lm can't take it
_sys_cache = None         # KV cache holding the static system prompt prefix
_sys_token_ids = []       # the tokens that prefix represents
_sys_len = 0


def load_model():
    global _model, _tokenizer, _draft_model
    if _model is None:
        # Imported here so the script can be syntax-checked / linted without
        # mlx installed.
        from mlx_lm import load
        log(f"Loading MLX model: {config.MODEL}")
        _model, _tokenizer = load(config.MODEL)

        if config.DRAFT_MODEL:
            try:
                log(f"Loading draft model: {config.DRAFT_MODEL}")
                _draft_model, _ = load(config.DRAFT_MODEL)
            except Exception as e:  # noqa: BLE001
                log(
                    f"WARNING: draft model failed to load ({e}); "
                    "continuing without speculative decoding"
                )
                _draft_model = None

        _build_system_cache()
        log("Model loaded.")
    return _model, _tokenizer


def _build_system_cache():
    """
    Pre-compute the KV cache for the static system prompt (the authoring guide
    + instructions) once, so it isn't re-prefilled on every request. Best
    effort: on any failure we fall back to prefilling the full prompt.
    """
    global _sys_cache, _sys_token_ids, _sys_len
    try:
        import mlx.core as mx
        from mlx_lm.models.cache import make_prompt_cache

        # Tokenize the system prefix the same way run_model tokenizes the full
        # prompt (apply_chat_template), so the cached ids are a true prefix of it.
        _sys_token_ids = list(_tokenizer.apply_chat_template(
            [{"role": "system", "content": system_prompt()}],
            add_generation_prompt=False,
            tokenize=True,
        ))
        prompt = mx.array(_sys_token_ids)[None]

        # Prefill in chunks rather than one shot. A single forward pass over all
        # ~8.7k tokens spikes activation memory hard — enough to get the process
        # OOM-killed (Jetsam SIGKILL) on a 24GB machine running the 14B. Feeding
        # the cache CHUNK_TOKENS at a time, eval'ing between chunks, builds the
        # identical KV cache while keeping the transient footprint bounded.
        chunk = config.PREFILL_CHUNK_TOKENS

        def _prefill(model, cache):
            for i in range(0, prompt.shape[1], chunk):
                model(prompt[:, i:i + chunk], cache=cache)
                mx.eval([c.state for c in cache])

        # Prefill the prefix into the MAIN model's cache, and — when speculative
        # decoding is on — the DRAFT model's cache too. mlx-lm's speculative path
        # splits a passed prompt_cache into cache[:len(model.layers)] (main) and
        # cache[len(model.layers):] (draft); if the draft half is missing it hits
        # an IndexError, the fast path fails, and we re-prefill the whole guide on
        # every request. So the combined cache must carry both halves.
        model_cache = make_prompt_cache(_model)
        _prefill(_model, model_cache)
        caches = list(model_cache)

        if _draft_model is not None and _draft_ok:
            draft_cache = make_prompt_cache(_draft_model)
            _prefill(_draft_model, draft_cache)
            caches += list(draft_cache)

        mx.eval([c.state for c in caches])
        _sys_cache = caches
        _sys_len = len(_sys_token_ids)
        _kind = 'main+draft' if (_draft_model is not None and _draft_ok) else 'main'
        log(f"Cached system prefix ({_sys_len} tokens, {_kind} cache).")
    except Exception as e:  # noqa: BLE001
        log(
            f"WARNING: could not cache system prompt ({e}); "
            "prefilling it on each request"
        )
        _sys_cache = None
        _sys_token_ids = []
        _sys_len = 0


def _draft_kwargs():
    if _draft_model is not None and _draft_ok:
        return {"draft_model": _draft_model, "num_draft_tokens": config.NUM_DRAFT_TOKENS}
    return {}


def _stream(prompt, prompt_cache, max_tokens=None):
    """Run stream_generate, transparently dropping draft kwargs if unsupported."""
    global _draft_ok
    from mlx_lm import stream_generate

    kwargs = {"max_tokens": max_tokens or config.MAX_TOKENS}
    if prompt_cache is not None:
        kwargs["prompt_cache"] = prompt_cache
    kwargs.update(_draft_kwargs())

    def go():
        return "".join(r.text for r in stream_generate(_model, _tokenizer, prompt, **kwargs))

    try:
        return go()
    except TypeError as e:
        if "draft_model" in str(e) or "num_draft_tokens" in str(e):
            _draft_ok = False
            kwargs.pop("draft_model", None)
            kwargs.pop("num_draft_tokens", None)
            return go()
        raise


def run_model(source: str, user_prompt: str, prev_attempt: dict = None, max_tokens=None, mode: str = None) -> str:
    """Run the model and return the raw generated text."""
    model, tokenizer = load_model()
    messages = build_messages(source, user_prompt, prev_attempt, mode)
    full_ids = list(tokenizer.apply_chat_template(messages, add_generation_prompt=True))

    # Bound total context (prompt + generated) to cap KV-cache memory. The cache
    # grows one entry per token, so peak KV memory ~= MAX_CONTEXT_TOKENS tokens.
    # Reject inputs that already overflow it (fail fast instead of OOM-killing),
    # and clamp generation so prompt + output stays within the window. max_tokens
    # is only passed explicitly by warmup, which stays tiny — leave it alone then.
    if max_tokens is None:
        budget = config.MAX_CONTEXT_TOKENS - len(full_ids)
        if budget <= 0:
            raise ValueError(
                f"prompt is {len(full_ids)} tokens, over MAX_CONTEXT_TOKENS "
                f"({config.MAX_CONTEXT_TOKENS}); shrink the game source or raise the cap"
            )
        max_tokens = min(config.MAX_TOKENS, budget)
        log(f"context budget: prompt={len(full_ids)} tokens, "
            f"max_new={max_tokens} (window={config.MAX_CONTEXT_TOKENS})")

    # Fast path: reuse the cached system prefix, generating only the delta.
    if _sys_cache is not None and full_ids[:_sys_len] == _sys_token_ids:
        try:
            import mlx.core as mx
            from mlx_lm.models.cache import trim_prompt_cache

            delta = full_ids[_sys_len:]
            log(f"prompt cache HIT: reusing {_sys_len} cached system tokens, "
                f"prefilling {len(delta)} delta tokens")
            text = _stream(mx.array(delta), _sys_cache, max_tokens=max_tokens)
            # Restore every cache entry to the pristine system prefix for next
            # time. The main and draft halves can advance by different amounts
            # under speculative decoding, so trim each entry by its own offset
            # rather than assuming a single shared length.
            for c in _sys_cache:
                extra = c.offset - _sys_len
                if extra > 0:
                    trim_prompt_cache([c], extra)
            return text
        except Exception as e:  # noqa: BLE001
            log(f"WARNING: cached generation failed ({e}); full prefill")
            _build_system_cache()  # rebuild a possibly-corrupted cache
    else:
        reason = "no cache" if _sys_cache is None else "prefix mismatch"
        log(f"prompt cache MISS ({reason}): prefilling full {len(full_ids)} tokens")

    # Fallback: prefill the whole prompt. Always correct.
    full_text = tokenizer.apply_chat_template(
        messages, add_generation_prompt=True, tokenize=False
    )
    return _stream(full_text, None, max_tokens=max_tokens)


def _warmup():
    """
    Run a tiny end-to-end generation so the first real request doesn't pay
    MLX's one-time, lazy Metal-kernel compilation for the decode + speculative
    path. Exercises the same fast-path machinery as a real job (cache reuse,
    draft verification, cache trim) and leaves the system cache pristine. Best
    effort: a warmup failure must not stop the server from serving.
    """
    try:
        log("warming generation path...")
        run_model("module.exports = {};", "noop", max_tokens=4)
        log("warmup complete.")
    except Exception as e:  # noqa: BLE001
        log(f"WARNING: warmup failed ({e}); first request will be slower")


def validate_js(code: str) -> str | None:
    """
    Validate generated index.js. Returns an error string if invalid,
    or None if it passes. Uses `node --check` for a real parse.
    """
    if not code.strip():
        return "Model returned empty output"
    if "module.exports" not in code:
        return "Output does not export a game (no module.exports)"

    with tempfile.NamedTemporaryFile(
        "w", suffix=".js", delete=False, encoding="utf-8"
    ) as f:
        f.write(code)
        tmp_path = f.name
    try:
        proc = subprocess.run(
            ["node", "--check", tmp_path],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if proc.returncode != 0:
            return f"Generated code is not valid JavaScript: {proc.stderr.strip()[:500]}"
    except FileNotFoundError:
        # node not installed — skip the parse check rather than failing hard.
        log("WARNING: `node` not found; skipping syntax validation")
    except subprocess.TimeoutExpired:
        return "Syntax validation timed out"
    finally:
        os.unlink(tmp_path)
    return None


def process_job(job: dict) -> dict:
    """
    Run one generation job. Returns a result dict (without the id, which the
    caller fills in): {"status": "COMPLETED", "result": code} or
    {"status": "FAILED", "error": message}.
    """
    source = job.get("source", "")
    user_prompt = job.get("prompt", "")
    mode = job.get("mode")
    log(f"Processing {job.get('id')} (mode={mode or 'EDIT'}): {user_prompt[:80]!r}")

    prev_attempt = None
    last_error = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            raw = run_model(source, user_prompt, prev_attempt, mode=mode)
        except Exception as e:  # noqa: BLE001 - report any failure back to the user
            last_error = str(e)[:500]
            log(f"  -> generation error: {last_error}")
            break

        code = extract_code(raw)
        err = validate_js(code)
        if err is None:
            log(f"  -> success on attempt {attempt + 1} ({len(code)} chars)")
            return {"status": "COMPLETED", "result": code}

        last_error = err
        prev_attempt = {"code": code, "error": err}
        log(f"  -> attempt {attempt + 1} failed validation: {err}")

    return {"status": "FAILED", "error": last_error or "unknown error"}


def _emit(obj: dict):
    """Write exactly one protocol line to stdout."""
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    # Warm the model AND the generation path before announcing readiness, so the
    # first real job isn't slow: load_model() builds the system-prompt cache,
    # _warmup() compiles the lazy decode/speculative kernels.
    load_model()
    _warmup()
    _emit({"ready": True})
    log("Model server ready; reading jobs on stdin.")

    # One job per line. The parent serializes requests (the model is the
    # bottleneck), so we process synchronously and reply in order.
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        job_id = None
        try:
            job = json.loads(line)
            job_id = job.get("id")
            result = process_job(job)
            result["id"] = job_id
            _emit(result)
        except json.JSONDecodeError:
            log("Discarding malformed stdin line")
            # No id to correlate; the parent's per-request timeout covers this.
        except Exception as e:  # noqa: BLE001 - never die on a single bad job
            log(f"Unexpected error handling job {job_id}: {e}")
            _emit({"id": job_id, "status": "FAILED", "error": str(e)[:500]})

    log("stdin closed; model server exiting.")


if __name__ == "__main__":
    main()
