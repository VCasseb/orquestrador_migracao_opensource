from __future__ import annotations

from migrate.core.credentials import get_env

DEFAULT_MODEL = "claude-sonnet-4-6"


def complete(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str]:
    api_key = get_env("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    model = get_env("ANTHROPIC_MODEL", DEFAULT_MODEL)
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=model, max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    return text, model


def test_connection() -> tuple[bool, str, str]:
    api_key = get_env("ANTHROPIC_API_KEY")
    if not api_key:
        return False, "Missing ANTHROPIC_API_KEY", ""
    model = get_env("ANTHROPIC_MODEL", DEFAULT_MODEL)
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        client.messages.create(model=model, max_tokens=1,
                               messages=[{"role": "user", "content": "hi"}])
    except Exception as e:
        return False, "Request failed", str(e)
    return True, f"API responded with model {model}", ""
