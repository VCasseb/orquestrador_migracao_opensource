from __future__ import annotations

from migrate.core.credentials import get_env

DEFAULT_MODEL = "gpt-4o-mini"


def complete(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str]:
    api_key = get_env("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    model = get_env("OPENAI_MODEL", DEFAULT_MODEL)
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model, max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    text = (resp.choices[0].message.content or "").strip()
    return text, model


def test_connection() -> tuple[bool, str, str]:
    api_key = get_env("OPENAI_API_KEY")
    if not api_key:
        return False, "Missing OPENAI_API_KEY", ""
    model = get_env("OPENAI_MODEL", DEFAULT_MODEL)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        client.chat.completions.create(
            model=model, max_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
    except Exception as e:
        return False, "Request failed", str(e)
    return True, f"API responded with model {model}", ""
