"""Prompt assembly and output extraction for the LLM worker."""

import functools
import re

import config

SYSTEM_INSTRUCTIONS = """\
You are an expert Homegames game developer. Homegames games are written in \
JavaScript using the squish.js library. You will be given the current contents \
of a game's index.js and a change request from the game's author.

Rewrite index.js to satisfy the request. Follow these rules strictly:
- Output the COMPLETE new index.js, not a diff or a fragment.
- Preserve everything that the request does not ask you to change.
- The file must export the game class via `module.exports` and remain valid, \
runnable squish.js per the authoring guide below.
- Do not add explanations. Output ONLY the code, in a single ```javascript code \
block.

Below is the authoritative squish.js authoring guide. Treat it as the contract \
for valid games:

---
{authoring_doc}
---
"""


@functools.lru_cache(maxsize=1)
def _authoring_doc() -> str:
    try:
        with open(config.AUTHORING_DOC_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except OSError:
        print(
            f"WARNING: authoring guide not found at {config.AUTHORING_DOC_PATH}; "
            "proceeding without it.",
            flush=True,
        )
        return "(authoring guide unavailable)"


def system_prompt() -> str:
    """The static system prompt. Identical every request, so it can be cached."""
    return SYSTEM_INSTRUCTIONS.format(authoring_doc=_authoring_doc())


def build_messages(source: str, user_prompt: str, prev_attempt: dict = None) -> list[dict]:
    """
    Build the chat messages for the model. The system message is constant
    (see system_prompt) so the worker can cache its KV state.

    prev_attempt, when given, is {"code": str, "error": str} from a failed
    validation pass, fed back so the model can correct itself.
    """
    user = (
        "Here is the current index.js:\n\n"
        "```javascript\n"
        f"{source}\n"
        "```\n\n"
        f"Change request:\n{user_prompt}\n\n"
        "Output the complete updated index.js."
    )
    if prev_attempt:
        user += (
            "\n\nYour previous attempt failed validation with this error:\n"
            f"{prev_attempt.get('error', '')}\n"
            "Fix the problem and output the complete, corrected index.js."
        )
    return [
        {"role": "system", "content": system_prompt()},
        {"role": "user", "content": user},
    ]


_FENCE_RE = re.compile(r"```(?:javascript|js)?\s*\n(.*?)```", re.DOTALL)


def extract_code(raw: str) -> str:
    """
    Pull the code out of the model's response. Prefers the first fenced
    block; falls back to the raw text if no fence is present.
    """
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    return raw.strip()
