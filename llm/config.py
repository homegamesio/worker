"""
Configuration for the LLM model server, read from the environment.

This process is a pure compute worker: it loads an MLX model and answers
generation requests on stdin/stdout. AMQP, the Homegames API, and the worker
secret are owned by the Node parent (index.js), so none of that lives here
anymore.
"""

import os

# --- Model ----------------------------------------------------------------
# Any MLX-compatible model id or local path. A 7B code model is the default:
# it fits comfortably in 32GB and is the realistic path to ~30s responses.
# Benchmark alternatives (Qwen3-Coder, etc.) by swapping this env var.
MODEL = os.environ.get(
    "MODEL", "mlx-community/Qwen2.5-Coder-14B-Instruct-4bit"
)

# Optional small draft model for speculative decoding. A tiny same-family
# model (e.g. 0.5B) proposes tokens the main model verifies — typically
# 1.5-2x faster decode with no quality loss. Set to "" to disable.
DRAFT_MODEL = os.environ.get(
    "DRAFT_MODEL", "mlx-community/Qwen2.5-Coder-0.5B-Instruct-4bit"
)
NUM_DRAFT_TOKENS = int(os.environ.get("NUM_DRAFT_TOKENS", "3"))

MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "8192"))

# Hard cap on total context (prompt + generated tokens), to bound KV-cache
# memory — peak KV use scales with this. The system prompt alone is ~8.7k
# tokens, so keep this comfortably above that. Generation is clamped so that
# prompt + output stays within the window; a prompt that already exceeds it is
# rejected rather than risking an OOM kill. Lower it to relieve memory pressure
# (at the cost of how large a game + rewrite can be handled).
MAX_CONTEXT_TOKENS = int(os.environ.get("MAX_CONTEXT_TOKENS", "16384"))

# Tokens per chunk when prefilling the system prompt into the KV cache at
# startup. Prefilling the whole ~8.7k-token guide in one forward pass spikes
# activation memory enough to get OOM-killed on smaller machines; chunking
# bounds that. Lower it if the model server is still being SIGKILLed at startup.
PREFILL_CHUNK_TOKENS = int(os.environ.get("PREFILL_CHUNK_TOKENS", "4000"))

# How many times to regenerate if the output fails validation. Each retry
# feeds the validation error back to the model. 0 = no retries.
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))

# Path to the squish.js authoring guide that grounds the model. The single
# source of truth lives in homegames-common; the Node parent (index.js) resolves
# it and passes AUTHORING_DOC_PATH in the environment. The fallback below only
# applies to standalone runs and intentionally has no in-repo copy — if it's
# missing, prompts.py proceeds without the guide (and warns).
AUTHORING_DOC_PATH = '/Users/josephgarcia/homegames/homegames-common/docs/squishjs-game-authoring.md'
#os.environ.get(
 #   "AUTHORING_DOC_PATH",
  #  os.path.join(os.path.dirname(__file__), "squishjs-game-authoring.md"),
#)
