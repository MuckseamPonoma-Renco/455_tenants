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

def llm_enabled() -> bool:
    mode = (_get_env("LLM_MODE", "") or "").strip().lower()
    if mode in {"0", "false", "no", "off"}:
        return False
    return bool((_api_key() or "").strip())

def _headers() -> dict:
    key = _api_key()
    if not key:
        raise OpenAIError("OPENAI_API_KEY (or LLM_API_KEY) is not set")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

def _extract_json(text: str) -> dict:
    if not isinstance(text, str) or not text.strip():
        raise OpenAIError("invalid_json_output")
    try:
        result = json.loads(text)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            raise OpenAIError("invalid_json_output") from None
        try:
            result = json.loads(m.group(0))
        except (TypeError, ValueError):
            raise OpenAIError("invalid_json_output") from None
    if not isinstance(result, dict):
        raise OpenAIError("invalid_json_output")
    return result


def _api_error(response) -> OpenAIError:
    try:
        error = response.json().get("error", {})
        code = str(error.get("code") or error.get("type") or "api_error")
    except Exception:
        code = "api_error"
    # Never echo a remote body containing chat text, credentials or HTML.
    code = code if re.fullmatch(r"[a-zA-Z0-9_]{1,80}", code) else "api_error"
    from packages.llm.provider_health import record
    record(code)
    return OpenAIError(f"OpenAI API error: {response.status_code} {code}")

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.7, min=0.7, max=4),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
)
def call_openai_json(prompt: str, model: str, max_output_tokens: int = 220, *, response_schema: dict | None = None, instructions: str | None = None, reasoning_effort: str | None = None) -> dict:
    """Return parsed JSON from OpenAI.

    Tries Responses API first; falls back to Chat Completions.
    """
    from packages.llm.provider_health import deferred_error
    if code := deferred_error():
        raise OpenAIError('OpenAI API temporarily blocked: '+code)
    url = _base_url().rstrip("/")
    headers = _headers()

    with httpx.Client(timeout=20.0) as client:
        # Responses API (preferred)
        request = {
            "model": model,
            "input": prompt,
            "max_output_tokens": max_output_tokens,
        }
        if instructions is not None:
            request['instructions'] = instructions
        if reasoning_effort is not None:
            request['reasoning'] = {'effort': reasoning_effort}
        if response_schema is not None:
            request["text"] = {"format": {"type": "json_schema", "name": "tenant_triage", "strict": True, "schema": response_schema}}
        r = client.post(f"{url}/responses", headers=headers, json=request)
        if r.status_code < 300:
            from packages.llm.provider_health import record
            record()
            try:
                data = r.json()
            except ValueError:
                raise OpenAIError("invalid_json_output") from None
            if not isinstance(data, dict):
                raise OpenAIError("invalid_json_output")
            if data.get("status") not in {None, "completed"}:
                raise OpenAIError("incomplete_output")
            if "output_text" in data and isinstance(data["output_text"], str):
                return _extract_json(data["output_text"])
            out = []
            items = data.get("output", []) or []
            if not isinstance(items, list):
                raise OpenAIError("invalid_json_output")
            for item in items:
                if not isinstance(item, dict):
                    raise OpenAIError("invalid_json_output")
                contents = item.get("content", []) or []
                if not isinstance(contents, list):
                    raise OpenAIError("invalid_json_output")
                for c in contents:
                    if not isinstance(c, dict):
                        raise OpenAIError("invalid_json_output")
                    if c.get("type") == "refusal":
                        raise OpenAIError("model_refusal")
                    if c.get("type") in ("output_text", "text") and "text" in c:
                        if not isinstance(c["text"], str):
                            raise OpenAIError("invalid_json_output")
                        out.append(c["text"])
            if out:
                return _extract_json("\n".join(out))
            raise OpenAIError("Responses API returned no text output")

        # Only an unavailable endpoint justifies a compatibility fallback.
        # Billing, authentication and request errors must not trigger a second call.
        if r.status_code not in {404, 405, 501}:
            raise _api_error(r)
        request = {
            "model": model,
            "messages": [
                {"role": "system", "content": instructions or "You output ONLY valid JSON. No markdown, no extra text."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": max_output_tokens,
        }
        if response_schema is not None:
            request["response_format"] = {"type": "json_schema", "json_schema": {"name": "tenant_triage", "strict": True, "schema": response_schema}}
        if reasoning_effort is not None:
            request.pop('temperature')
            request['max_completion_tokens'] = request.pop('max_tokens')
            request['reasoning_effort'] = reasoning_effort
        r2 = client.post(f"{url}/chat/completions", headers=headers, json=request)
        if r2.status_code >= 300:
            raise _api_error(r2)
        from packages.llm.provider_health import record
        record()
        try:
            data2 = r2.json()
            choice = data2["choices"][0]
            if choice.get("finish_reason") == "length":
                raise OpenAIError("incomplete_output")
            if choice["message"].get("refusal"):
                raise OpenAIError("model_refusal")
            text = choice["message"]["content"]
        except (ValueError, KeyError, TypeError, IndexError, AttributeError):
            raise OpenAIError("invalid_json_output") from None
        return _extract_json(text)
