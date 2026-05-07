"""AWS Bedrock provider — supports Anthropic Claude models via Bedrock's
Anthropic message format (`anthropic_version: bedrock-2023-05-31`).

Other model families (Llama, Mistral, Titan) use different request shapes
and are not supported here yet — pin BEDROCK_MODEL_ID to a Claude model.

AWS auth uses the standard boto3 credential chain: env vars
(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), ~/.aws/credentials, IAM role, etc.
Region defaults to AWS_REGION env var.
"""
from __future__ import annotations

import json

from migrate.core.credentials import get_env

DEFAULT_MODEL = "anthropic.claude-sonnet-4-20250514-v1:0"
DEFAULT_REGION = "us-east-1"


def complete(system: str, user: str, max_tokens: int = 4000) -> tuple[str, str]:
    region = get_env("AWS_REGION", DEFAULT_REGION) or DEFAULT_REGION
    model = get_env("BEDROCK_MODEL_ID", DEFAULT_MODEL)
    import boto3
    client = boto3.client("bedrock-runtime", region_name=region)
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    })
    resp = client.invoke_model(modelId=model, body=body)
    payload = json.loads(resp["body"].read())
    parts = payload.get("content", [])
    text = "".join(p["text"] for p in parts if p.get("type") == "text").strip()
    return text, model


def test_connection() -> tuple[bool, str, str]:
    region = get_env("AWS_REGION", DEFAULT_REGION) or DEFAULT_REGION
    model = get_env("BEDROCK_MODEL_ID", DEFAULT_MODEL)
    try:
        import boto3
        client = boto3.client("bedrock-runtime", region_name=region)
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "hi"}],
        })
        client.invoke_model(modelId=model, body=body)
    except Exception as e:
        return False, "Request failed", str(e)
    return True, f"Bedrock {region} responded with {model}", ""
