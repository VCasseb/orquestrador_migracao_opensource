"""Unified LLM dispatcher.

Reads LLM_PROVIDER env var (anthropic | openai | gemini | bedrock) and routes
complete() / test_connection() to the right provider implementation.

Per-provider env vars:
  anthropic: ANTHROPIC_API_KEY, ANTHROPIC_MODEL
  openai:    OPENAI_API_KEY,    OPENAI_MODEL
  gemini:    GEMINI_API_KEY,    GEMINI_MODEL
  bedrock:   AWS_REGION, BEDROCK_MODEL_ID, AWS_ACCESS_KEY_ID/SECRET (or default chain)
"""
from __future__ import annotations

from dataclasses import dataclass

from migrate.core.credentials import get_env, load_env

PROVIDERS = ("anthropic", "openai", "gemini", "bedrock")


@dataclass
class LLMResult:
    text: str
    model: str


def active_provider() -> str:
    load_env()
    p = (get_env("LLM_PROVIDER", "anthropic") or "anthropic").lower().strip()
    return p if p in PROVIDERS else "anthropic"


def _provider_module(provider: str | None = None):
    provider = provider or active_provider()
    if provider == "anthropic":
        from migrate.core.llm import anthropic_provider as p
    elif provider == "openai":
        from migrate.core.llm import openai_provider as p
    elif provider == "gemini":
        from migrate.core.llm import gemini_provider as p
    elif provider == "bedrock":
        from migrate.core.llm import bedrock_provider as p
    else:
        raise RuntimeError(f"Unknown LLM_PROVIDER: {provider}")
    return p


def complete(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str]:
    """Run a single completion against the active provider. Returns (text, model)."""
    return _provider_module().complete(system, user, max_tokens)


def complete_with_usage(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str, dict | None]:
    """Like complete(), but also returns token usage {input_tokens, output_tokens}
    (or None if the provider/response didn't report it)."""
    return _provider_module().complete_with_usage(system, user, max_tokens)


def active_model(provider: str | None = None) -> str:
    """The model id used for the given (or active) provider — read from env, no API
    call. Lets us price/estimate without credentials."""
    provider = provider or active_provider()
    env_key, default = {
        "anthropic": ("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
        "openai": ("OPENAI_MODEL", "gpt-4o-mini"),
        "gemini": ("GEMINI_MODEL", "gemini-2.0-flash"),
        "bedrock": ("BEDROCK_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0"),
    }.get(provider, ("ANTHROPIC_MODEL", "claude-sonnet-4-6"))
    return get_env(env_key, default) or default


def test_connection(provider: str | None = None) -> tuple[bool, str, str]:
    """Test the given provider (or active one). Returns (ok, message, detail)."""
    provider = (provider or active_provider()).lower()
    try:
        if provider == "anthropic":
            from migrate.core.llm import anthropic_provider as p
        elif provider == "openai":
            from migrate.core.llm import openai_provider as p
        elif provider == "gemini":
            from migrate.core.llm import gemini_provider as p
        elif provider == "bedrock":
            from migrate.core.llm import bedrock_provider as p
        else:
            return False, f"Unknown provider: {provider}", ""
        return p.test_connection()
    except Exception as e:
        return False, "Provider import / test failed", str(e)
