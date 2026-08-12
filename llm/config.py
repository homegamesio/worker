"""
Configuration for the LLM model server, read from the environment.

This process is a thin bridge: it forwards generation requests to an
OpenAI-compatible server (LM Studio) over HTTP and answers on stdin/stdout.
AMQP, the Homegames API, and the worker secret are owned by the Node parent
(index.js), so none of that lives here.
"""

import os

# --- LM Studio server -------------------------------------------------------
# Base URL of the OpenAI-compatible API. LM Studio's default is port 1234;
# enable it under Developer > "Start Server" (or `lms server start`).
LLM_SERVER_URL = os.environ.get("LLM_SERVER_URL", "http://localhost:1234/v1").rstrip("/")

# Model name to request, as LM Studio reports it (see GET /v1/models).
# Leave empty to auto-select the first model the server lists.
#
# IMPORTANT: load the model with a context length of at least ~45k tokens.
# Every request (game gen AND docs) carries the shared ~29k-token system
# prompt (knowledge doc + authoring guide; see prompts.py), plus the game's
# source and up to MAX_TOKENS of output. Too small a context 400s at warmup.
MODEL = os.environ.get("MODEL", "google/gemma-4-26b-a4b")

# Hard deadline for one generation request. A big model producing a full game
# can legitimately take several minutes; this only exists so a hung server
# fails the job instead of wedging the pipeline forever.
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "900"))

# Total wall-clock budget for one game-generation job INCLUDING validation
# retries. Must stay comfortably under the Node parent's 10-minute LLM job
# backstop (index.js): if the child blows past that, Node SIGKILLs the model
# server and the warm model/KV cache dies with it. Instead the child stops
# retrying when this budget runs out and reports FAILED, which still gets
# posted back to the API.
JOB_DEADLINE_SECONDS = int(os.environ.get("JOB_DEADLINE_SECONDS", "540"))

# Same idea for docs questions, under Node's 5-minute docs backstop.
DOCS_DEADLINE_SECONDS = int(os.environ.get("DOCS_DEADLINE_SECONDS", "270"))

# --- Generation -------------------------------------------------------------
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "8192"))

# 0 = deterministic. Matches the greedy decoding the old MLX path used.
TEMPERATURE = float(os.environ.get("TEMPERATURE", "0"))

# How many times to regenerate if the output fails validation. Each retry
# feeds the validation error back to the model. 0 = no retries.
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))

# Path to the squish.js authoring guide that grounds the model. The single
# source of truth lives in homegames-common; the Node parent (index.js) resolves
# it and passes AUTHORING_DOC_PATH in the environment. The fallback below only
# applies to standalone runs and intentionally has no in-repo copy — if it's
# missing, prompts.py proceeds without the guide (and warns).
AUTHORING_DOC_PATH = os.environ.get(
    "AUTHORING_DOC_PATH",
    os.path.join(os.path.dirname(__file__), "squishjs-game-authoring.md"),
)

# Path to the whole-platform knowledge doc that grounds docs-question answers
# ("ask something" box on homegames.io/docs.html). Same arrangement as the
# authoring guide: canonical copy in homegames-common, resolved by the parent.
KNOWLEDGE_DOC_PATH = os.environ.get(
    "KNOWLEDGE_DOC_PATH",
    os.path.join(os.path.dirname(__file__), "homegames-knowledge.md"),
)

# Docs answers are a paragraph or two, not a whole game file.
DOCS_MAX_TOKENS = int(os.environ.get("DOCS_MAX_TOKENS", "1024"))
