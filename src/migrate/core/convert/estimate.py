"""Local token + cost estimation for code conversions.

100% offline: NO API key and NO network call required. Uses a character-based
heuristic (~3.6 chars/token for code) plus a per-model price table to estimate
how many tokens (and dollars) a conversion would consume BEFORE running it.

For EXACT counts, run the real conversion — providers now report token usage
(see complete_with_usage) which is surfaced as the artifact's actual tokens.
"""
from __future__ import annotations

import math
from typing import Any

from migrate.core.convert.code import _SYSTEM_PROMPTS
from migrate.core.llm import active_model, active_provider

# Average characters per token for source code (empirically ~3.3–3.8). Slightly
# conservative so estimates don't undershoot.
CHARS_PER_TOKEN = 3.6

# Fixed user-message wrapper overhead (the "Convert this ... Reply with ONLY ..."
# scaffolding around the source), measured at ~38 tokens.
USER_WRAPPER_TOKENS = 38

# Output tends to be roughly the size of the converted code. For these
# rewrites it lands around 1.0–1.5x the source; 1.2 is a sane middle estimate.
OUTPUT_RATIO = 1.2

# Hard cap per call (mirrors max_tokens in code._llm_call).
OUTPUT_CAP = 8000

# Prices in USD per 1,000,000 tokens (input, output). Approximate public list
# prices — adjust to your contract. Matched by substring against the model id.
PRICES: dict[str, tuple[float, float]] = {
    "claude-opus": (15.0, 75.0),
    "claude-sonnet": (3.0, 15.0),
    "claude-haiku": (0.80, 4.0),
    "claude-3-5-haiku": (0.80, 4.0),
    "claude-3-haiku": (0.25, 1.25),
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.0),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1": (2.0, 8.0),
    "o4-mini": (1.10, 4.40),
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-1.5-flash": (0.075, 0.30),
    "gemini-1.5-pro": (1.25, 5.0),
}
DEFAULT_PRICE = (3.0, 15.0)  # sonnet-class fallback


def estimate_tokens(text: str) -> int:
    """Heuristic token count for a chunk of text/code."""
    if not text:
        return 0
    return math.ceil(len(text) / CHARS_PER_TOKEN)


def price_for(model: str) -> tuple[float, float, bool]:
    """Returns (input_per_1m, output_per_1m, known). `known` is False when we fell
    back to the default because the model id wasn't in the table."""
    m = (model or "").lower()
    # Longest-key-first so 'claude-3-5-haiku' beats 'claude-haiku'.
    for key in sorted(PRICES, key=len, reverse=True):
        if key in m:
            pin, pout = PRICES[key]
            return pin, pout, True
    pin, pout = DEFAULT_PRICE
    return pin, pout, False


def _system_for(source_type: str, target_platform: str, custom_prompt: str = "") -> str:
    system = _SYSTEM_PROMPTS.get((source_type, target_platform), "")
    if custom_prompt.strip():
        system = system + "\n\n# Project-specific instructions from the user\n" + custom_prompt.strip()
    return system


def estimate_conversion(
    source_type: str,
    target_platform: str,
    source_code: str,
    custom_prompt: str = "",
    model: str | None = None,
) -> dict[str, Any]:
    """Estimate tokens + cost for converting one code artifact. No API key needed."""
    model = model or active_model()
    system = _system_for(source_type, target_platform, custom_prompt)

    sys_tok = estimate_tokens(system)
    src_tok = estimate_tokens(source_code)
    input_tokens = sys_tok + USER_WRAPPER_TOKENS + src_tok
    output_tokens = min(OUTPUT_CAP, max(64, round(src_tok * OUTPUT_RATIO)))
    total_tokens = input_tokens + output_tokens

    pin, pout, known = price_for(model)
    cost = input_tokens / 1_000_000 * pin + output_tokens / 1_000_000 * pout

    return {
        "provider": active_provider(),
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cost_usd": round(cost, 4),
        "price_known": known,
        "price_in_per_1m": pin,
        "price_out_per_1m": pout,
        "estimate": True,
    }


def source_text_for(inv, code_type: str, name: str) -> tuple[str | None, str | None]:
    """Fetch the raw source for an inventory item and its conversion source_type.
    Returns (code, source_type) or (None, None) if not found.
    code_type: 'notebook' | 'dag'.
    """
    if not inv:
        return None, None
    if code_type == "notebook":
        nb = inv.notebooks_by_id.get(name)
        if not nb:
            return None, None
        parts = []
        for c in nb.cells:
            marker = "# === MARKDOWN ===" if c.cell_type == "markdown" else "# === CODE ==="
            parts.append(f"{marker}\n{c.source}\n")
        return ("\n".join(parts) if parts else ""), "vertex_notebook"
    if code_type == "dag":
        d = inv.dags_by_id.get(name)
        if not d:
            return None, None
        return (d.source_code or ""), "airflow_dag"
    return None, None


def estimate_item(
    inv, code_type: str, name: str, target_platform: str, custom_prompt: str = "",
) -> dict[str, Any] | None:
    """Estimate a single inventory item by (code_type, name). None if not found."""
    code, source_type = source_text_for(inv, code_type, name)
    if source_type is None:
        return None
    est = estimate_conversion(source_type, target_platform, code or "", custom_prompt)
    est["name"] = name
    est["code_type"] = code_type
    return est
