#!/usr/bin/env python3
"""
Homegames LLM model server.

A thin bridge, driven by the Node parent (../index.js). It forwards "modify my
game" generation requests to an OpenAI-compatible server (LM Studio) and
answers over a line-delimited JSON protocol on stdin/stdout:

    stdin  (one JSON object per line):
        {"id": "<requestId>", "source": "<current index.js>", "prompt": "<edit request>",
         "mode": "CREATE"?}  # mode CREATE = generate a new game from the starter template
        {"id": "<requestId>", "kind": "docs", "prompt": "<question>"}
                             # docs Q&A: short grounded answer, no code extraction/validation

    stdout (one JSON object per line):
        {"id": "...", "status": "COMPLETED", "result": "<new index.js or docs answer>"}
        {"id": "...", "status": "FAILED",    "error": "<message>"}

    Plus exactly one readiness line, emitted once the server is reachable and
    the model has answered a warmup request:
        {"ready": true}

CHANNEL DISCIPLINE: stdout carries protocol JSON ONLY. Every diagnostic goes to
stderr (see log()), so HTTP chatter and per-attempt logs can never corrupt the
protocol stream the parent is parsing.

AMQP, the Homegames API call, and the worker secret are owned by the Node
parent — this process talks to nothing but the local LM Studio server, using
only the standard library.
"""

import functools
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import config
from prompts import build_docs_messages, build_messages, extract_code

# All human-readable output goes to stderr; stdout is reserved for protocol.
log = functools.partial(print, file=sys.stderr, flush=True)


# Resolved once at startup: the model name to send with each request.
_model_name = None


def _http_json(path: str, payload: dict = None, timeout: int = 30) -> dict:
    """GET (payload=None) or POST JSON to the LM Studio server."""
    url = config.LLM_SERVER_URL + path
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"LM Studio returned {e.code} for {path}: {body}") from e


def wait_for_server():
    """
    Block until the LM Studio server answers /models, then resolve the model
    name to use. Retries forever: the parent treats "not ready" as "still
    loading", and LM Studio may simply not be started yet.
    """
    global _model_name
    delay = 2
    while True:
        try:
            listed = _http_json("/models").get("data", [])
            available = [m.get("id", "") for m in listed]
            if config.MODEL:
                _model_name = config.MODEL
                if config.MODEL not in available:
                    log(
                        f"WARNING: MODEL '{config.MODEL}' not in server's model list "
                        f"({', '.join(available) or 'empty'}); requesting it anyway "
                        "(LM Studio may JIT-load it)"
                    )
            elif available:
                _model_name = available[0]
            else:
                log("LM Studio is up but lists no models; load one and this will proceed...")
                time.sleep(delay)
                continue
            log(f"LM Studio reachable at {config.LLM_SERVER_URL}; using model: {_model_name}")
            return
        except Exception as e:  # noqa: BLE001
            log(
                f"LM Studio not reachable at {config.LLM_SERVER_URL} ({e}); "
                f"retrying in {delay}s. Is the server started (Developer > Start Server)?"
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


def run_model(source: str, user_prompt: str, prev_attempt: dict = None, max_tokens=None, mode: str = None, messages: list = None, timeout: float = None) -> str:
    """Send one chat completion to LM Studio and return the raw generated text.

    Callers either let this build the game-edit messages (default) or pass
    prebuilt `messages` (docs Q&A path). `timeout` bounds this one HTTP
    request; defaults to REQUEST_TIMEOUT_SECONDS.
    """
    if messages is None:
        messages = build_messages(source, user_prompt, prev_attempt, mode)
    payload = {
        "model": _model_name,
        "messages": messages,
        "max_tokens": max_tokens or config.MAX_TOKENS,
        "temperature": config.TEMPERATURE,
        "stream": False,
        # llama.cpp-style hint to reuse the KV cache for the shared prefix.
        # The ~29k-token document block at the top of the system prompt is
        # byte-identical across BOTH job types (see prompts.py), so game jobs
        # and docs questions reuse each other's prefill. Servers that don't
        # know the field ignore it.
        "cache_prompt": True,
    }
    start = time.monotonic()
    resp = _http_json("/chat/completions", payload, timeout=int(timeout or config.REQUEST_TIMEOUT_SECONDS))
    choices = resp.get("choices") or []
    if not choices:
        raise RuntimeError(f"LM Studio returned no choices: {json.dumps(resp)[:300]}")
    text = choices[0].get("message", {}).get("content") or ""
    usage = resp.get("usage", {})
    log(
        f"generation took {time.monotonic() - start:.1f}s "
        f"(prompt={usage.get('prompt_tokens', '?')} tok, "
        f"completion={usage.get('completion_tokens', '?')} tok, "
        f"finish={choices[0].get('finish_reason')})"
    )
    return text


def _warmup():
    """
    Run a tiny end-to-end generation before announcing readiness. LM Studio
    JIT-loads a model on its first request, which can take minutes for a big
    model — paying that here means the first real job doesn't. Best effort: a
    warmup failure must not stop the server from serving.

    Uses the docs messages, but since both job types share the same ~29k-token
    system-prompt prefix (see prompts.py), this one request prefills the KV
    prefix that game jobs AND docs questions both reuse.
    """
    try:
        log("warming up (may trigger LM Studio's model load; prefills the shared prompt prefix)...")
        run_model("", "", messages=build_docs_messages("noop"), max_tokens=4)
        log("warmup complete.")
    except Exception as e:  # noqa: BLE001
        log(
            f"WARNING: warmup failed ({e}); first request will be slower. "
            "If this is a context-length 400, reload the model in LM Studio "
            "with a context length of at least ~45k tokens."
        )


def validate_js(code: str) -> str | None:
    """
    Validate generated index.js. Returns an error string if invalid,
    or None if it passes. Uses `node --check` for a real parse.
    """
    if not code.strip():
        return "Model returned empty output"
    if "module.exports" not in code:
        return (
            "Your output was not a game file (no module.exports found). "
            "You must respond with the complete index.js source code in a "
            "```javascript code block — never with prose, questions, or "
            "explanations."
        )

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


def process_docs_job(job: dict) -> dict:
    """
    Answer one docs question ("ask something" box). No code extraction, no
    validation, no retries — just a short grounded answer.
    """
    question = job.get("prompt", "")
    log(f"Processing docs question {job.get('id')}: {question[:80]!r}")
    try:
        raw = run_model(
            "", "",
            messages=build_docs_messages(question),
            max_tokens=config.DOCS_MAX_TOKENS,
            # Fail here, under Node's 5-minute docs backstop, so the failure
            # is reported cleanly instead of Node killing the warm server.
            timeout=min(config.REQUEST_TIMEOUT_SECONDS, config.DOCS_DEADLINE_SECONDS),
        )
    except Exception as e:  # noqa: BLE001 - report any failure back
        err = str(e)[:500]
        log(f"  -> docs generation error: {err}")
        return {"status": "FAILED", "error": err}

    answer = raw.strip()
    if not answer:
        return {"status": "FAILED", "error": "Model returned empty answer"}
    log(f"  -> answered ({len(answer)} chars)")
    return {"status": "COMPLETED", "result": answer}


def process_job(job: dict) -> dict:
    """
    Run one generation job. Returns a result dict (without the id, which the
    caller fills in): {"status": "COMPLETED", "result": code} or
    {"status": "FAILED", "error": message}.

    kind "docs" = a docs question (see process_docs_job); anything else is a
    game-edit job.
    """
    if job.get("kind") == "docs":
        return process_docs_job(job)

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
    # Confirm LM Studio is reachable AND the model answers before announcing
    # readiness, so the first real job isn't slow (or doomed).
    wait_for_server()
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
