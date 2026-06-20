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

# How many times to regenerate if the output fails validation. Each retry
# feeds the validation error back to the model. 0 = no retries.
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))

# Path to the squish.js authoring guide that grounds the model.
AUTHORING_DOC_PATH = os.environ.get(
    "AUTHORING_DOC_PATH",
    os.path.join(os.path.dirname(__file__), "squishjs-game-authoring.md"),
)
