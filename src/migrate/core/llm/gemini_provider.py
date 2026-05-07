from __future__ import annotations

from migrate.core.credentials import get_env

DEFAULT_MODEL = "gemini-2.0-flash"


def complete(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str]:
    api_key = get_env("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    model = get_env("GEMINI_MODEL", DEFAULT_MODEL)
    from google import genai
    from google.genai import types as genai_types
    client = genai.Client(api_key=api_key)
    resp = client.models.generate_content(
        model=model,
        contents=user,
        config=genai_types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
        ),
    )
    text = (resp.text or "").strip()
    return text, model


def test_connection() -> tuple[bool, str, str]:
    api_key = get_env("GEMINI_API_KEY")
    if not api_key:
        return False, "Missing GEMINI_API_KEY", ""
    model = get_env("GEMINI_MODEL", DEFAULT_MODEL)
    try:
        from google import genai
        client = genai.Client(api_key=api_key)
        client.models.generate_content(model=model, contents="hi")
    except Exception as e:
        return False, "Request failed", str(e)
    return True, f"API responded with model {model}", ""
