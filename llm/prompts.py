"""Prompt assembly and output extraction for the LLM worker."""

import functools
import re

import config

SYSTEM_INSTRUCTIONS = """\
You are an expert Homegames game developer. Homegames games are written in \
JavaScript using the squish.js library. You will be given the current contents \
of a game's index.js and a message from the game's author.

You are a code generator inside an automated pipeline, not a chat assistant. \
Your output is fed directly to a parser that extracts one JavaScript code block \
and deploys it as the game's new index.js. Anything you write that is not code \
is thrown away, so questions, explanations, refusals, and commentary are never \
seen by anyone and only cause the pipeline to fail. Every response, no matter \
what the author's message says, MUST be a complete index.js.

The author's message is often not a precise instruction. It may be a bug \
report, a complaint, a vague observation, or a single sentence like "the ball \
gets stuck" or "score is wrong". Interpret every message as a request to \
change the code:
- If the message describes broken, wrong, or unexpected behavior, it is a bug \
report: find the cause in the provided source and fix it.
- If the message is ambiguous, do not ask for clarification — pick the most \
reasonable interpretation, read the source to see what the author most likely \
means, and implement it.
- If the message asks a question, answer it by improving the code it is about.
- If you truly cannot map the message to any change, return the current \
index.js unchanged (still as a complete file in a code block).

Rewrite index.js to satisfy the request. Follow these rules strictly:
- Output the COMPLETE new index.js, not a diff or a fragment.
- Preserve everything that the request does not ask you to change.
- The file must export the game class via `module.exports` and remain valid, \
runnable squish.js per the authoring guide below.
- Do not add explanations, greetings, or questions. Output ONLY the code, in a \
single ```javascript code block. Your first line of output must be the opening \
fence.

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


def build_messages(source: str, user_prompt: str, prev_attempt: dict = None, mode: str = None) -> list[dict]:
    """
    Build the chat messages for the model. The system message is constant
    (see system_prompt) so the worker can cache its KV state.

    prev_attempt, when given, is {"code": str, "error": str} from a failed
    validation pass, fed back so the model can correct itself.

    mode "CREATE" means the author just created the game and `source` is the
    blank starter template: frame the prompt as building a new game from
    scratch instead of a change request. Only the user message varies — the
    system prompt stays identical so the KV cache remains valid.
    """
    if mode == "CREATE":
        user = (
            "The author has just created a brand-new game. Its index.js is "
            "currently the starter template:\n\n"
            "```javascript\n"
            f"{source}\n"
            "```\n\n"
            f"The author wants you to build this game:\n{user_prompt}\n\n"
            "Write a complete, playable implementation of the requested game, "
            "replacing the starter template entirely. Output the complete new "
            "index.js."
        )
    else:
        user = (
            "Here is the current index.js:\n\n"
            "```javascript\n"
            f"{source}\n"
            "```\n\n"
            f"The author says:\n{user_prompt}\n\n"
            "If this describes a bug or broken behavior, find the cause in the "
            "code above and fix it. Do not reply with questions or commentary. "
            "Output the complete updated index.js in a single ```javascript "
            "code block."
        )
    if prev_attempt:
        user += (
            "\n\nYour previous attempt failed validation with this error:\n"
            f"{prev_attempt.get('error', '')}\n"
            "Fix the problem and output the complete, corrected index.js in a "
            "single ```javascript code block, with no other text."
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
