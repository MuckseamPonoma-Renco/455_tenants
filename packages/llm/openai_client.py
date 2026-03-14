from __future__ import annotations
import os, json, re
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class OpenAIError(RuntimeError):
    pass

def _get_env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def _base_url() -> str:
    return _get_env("OPENAI_BASE_URL", "https://api.openai.com/v1")  # type: ignore

def _api_key() -> str | None:
    return _get_env("OPENAI_API_KEY") or _get_env("LLM_API_KEY")

def _headers() -> dict:
    key = _api_key()
    if not key:
        raise OpenAIError("OPENAI_API_KEY (or LLM_API_KEY) is not set")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

def _extract_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            raise OpenAIError("Model did not return JSON")
        return json.loads(m.group(0))

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.7, min=0.7, max=4),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
)
def call_openai_json(prompt: str, model: str, max_output_tokens: int = 220) -> dict:
    """Return parsed JSON from OpenAI.

    Tries Responses API first; falls back to Chat Completions.
    """
    url = _base_url().rstrip("/")
    headers = _headers()

    with httpx.Client(timeout=20.0) as client:
        # Responses API (preferred)
        r = client.post(f"{url}/responses", headers=headers, json={
            "model": model,
            "input": prompt,
            "max_output_tokens": max_output_tokens,
        })
        if r.status_code < 300:
            data = r.json()
            if "output_text" in data and isinstance(data["output_text"], str):
                return _extract_json(data["output_text"])
            out = []
            for item in data.get("output", []) or []:
                for c in item.get("content", []) or []:
                    if c.get("type") in ("output_text", "text") and "text" in c:
                        out.append(c["text"])
            if out:
                return _extract_json("\n".join(out))
            raise OpenAIError("Responses API returned no text output")

        # Chat Completions fallback
        r2 = client.post(f"{url}/chat/completions", headers=headers, json={
            "model": model,
            "messages": [
                {"role": "system", "content": "You output ONLY valid JSON. No markdown, no extra text."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": max_output_tokens,
        })
        if r2.status_code >= 300:
            raise OpenAIError(f"OpenAI API error: {r2.status_code} {r2.text[:400]}")
        data2 = r2.json()
        text = data2["choices"][0]["message"]["content"]
        return _extract_json(text)
