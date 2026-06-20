#!/usr/bin/env python3
"""
Homegames LLM model server.

A pure compute worker, driven by the Node parent (../index.js). It loads a
local MLX model once (kept warm) and answers "modify my game" generation
requests over a line-delimited JSON protocol on stdin/stdout:

    stdin  (one JSON object per line):
        {"id": "<requestId>", "source": "<current index.js>", "prompt": "<edit request>"}

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

        sys_text = _tokenizer.apply_chat_template(
            [{"role": "system", "content": system_prompt()}],
            add_generation_prompt=False,
            tokenize=False,
        )
        _sys_token_ids = list(_tokenizer.encode(sys_text))
        _sys_cache = make_prompt_cache(_model)
        _model(mx.array(_sys_token_ids)[None], cache=_sys_cache)
        mx.eval([c.state for c in _sys_cache])
        _sys_len = len(_sys_token_ids)
        log(f"Cached system prefix ({_sys_len} tokens).")
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


def _stream(prompt, prompt_cache):
    """Run stream_generate, transparently dropping draft kwargs if unsupported."""
    global _draft_ok
    from mlx_lm import stream_generate

    kwargs = {"max_tokens": config.MAX_TOKENS}
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


def run_model(source: str, user_prompt: str, prev_attempt: dict = None) -> str:
    """Run the model and return the raw generated text."""
    model, tokenizer = load_model()
    messages = build_messages(source, user_prompt, prev_attempt)
    full_ids = list(tokenizer.apply_chat_template(messages, add_generation_prompt=True))

    # Fast path: reuse the cached system prefix, generating only the delta.
    if _sys_cache is not None and full_ids[:_sys_len] == _sys_token_ids:
        try:
            import mlx.core as mx
            from mlx_lm.models.cache import trim_prompt_cache

            delta = full_ids[_sys_len:]
            text = _stream(mx.array(delta), _sys_cache)
            # Restore the cache to the pristine system prefix for next time.
            extra = _sys_cache[0].offset - _sys_len
            if extra > 0:
                trim_prompt_cache(_sys_cache, extra)
            return text
        except Exception as e:  # noqa: BLE001
            log(f"WARNING: cached generation failed ({e}); full prefill")
            _build_system_cache()  # rebuild a possibly-corrupted cache

    # Fallback: prefill the whole prompt. Always correct.
    full_text = tokenizer.apply_chat_template(
        messages, add_generation_prompt=True, tokenize=False
    )
    return _stream(full_text, None)


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
    log(f"Processing {job.get('id')}: {user_prompt[:80]!r}")

    prev_attempt = None
    last_error = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            raw = run_model(source, user_prompt, prev_attempt)
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
    # Warm the model before announcing readiness so the first job isn't slow.
    load_model()
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
