"""
llm_runtime.py

Shared LLM runtime helpers:
- Provider-aware OpenAI client initialization (Mistral / Ollama)
- Thread-safe request throttling (max concurrency + min interval)
- Retry backoff helpers with jitter
"""

from __future__ import annotations

import os
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional, Tuple

from openai import OpenAI


def env_flag(name: str, default: bool = False) -> bool:
    """Parse a boolean-like env var."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def get_llm_provider() -> str:
    """Current provider key."""
    return os.getenv("LLM_PROVIDER", "mistral").strip().lower()


def get_llm_model() -> str:
    """Resolve model from provider-aware env defaults."""
    provider = get_llm_provider()
    if provider == "ollama":
        return os.getenv("OLLAMA_MODEL", os.getenv("LLM_MODEL", "llama3.2")).strip()
    return os.getenv("LLM_MODEL", "mistral-small-latest").strip()


def create_llm_client() -> OpenAI:
    """
    Build OpenAI-compatible client for configured provider.

    Providers:
    - mistral (default): uses MISTRAL_API_KEY + MISTRAL_BASE_URL
    - ollama: uses OLLAMA_BASE_URL + OLLAMA_API_KEY (default "ollama")
    """
    provider = get_llm_provider()
    if provider == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1").strip()
        api_key = os.getenv("OLLAMA_API_KEY", "ollama").strip()
        return OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

    api_key = os.getenv("MISTRAL_API_KEY", "").strip()
    base_url = os.getenv("MISTRAL_BASE_URL", "https://api.mistral.ai/v1").strip()
    return OpenAI(api_key=api_key, base_url=base_url, max_retries=0)


@dataclass(frozen=True)
class _LimiterConfig:
    max_concurrent: int
    min_interval_sec: float


class LLMRateLimiter:
    """Thread-safe gate for request pacing."""

    def __init__(self, max_concurrent: int, min_interval_sec: float) -> None:
        self._semaphore = threading.BoundedSemaphore(max(1, int(max_concurrent)))
        self._min_interval_sec = max(0.0, float(min_interval_sec))
        self._schedule_lock = threading.Lock()
        self._next_allowed_at = 0.0

    @contextmanager
    def slot(self) -> Generator[None, None, None]:
        """Acquire concurrency slot and respect global min interval."""
        self._semaphore.acquire()
        try:
            if self._min_interval_sec > 0:
                with self._schedule_lock:
                    now = time.monotonic()
                    wait_for = max(0.0, self._next_allowed_at - now)
                    self._next_allowed_at = max(self._next_allowed_at, now) + self._min_interval_sec
                if wait_for > 0:
                    time.sleep(wait_for)
            yield
        finally:
            self._semaphore.release()


_LIMITER_LOCK = threading.Lock()
_LIMITER_INSTANCE: Optional[LLMRateLimiter] = None
_LIMITER_CONFIG: Optional[_LimiterConfig] = None


def get_llm_rate_limiter() -> LLMRateLimiter:
    """Get process-wide limiter instance (re-created if config changes)."""
    global _LIMITER_INSTANCE, _LIMITER_CONFIG
    config = _LimiterConfig(
        max_concurrent=max(1, int(os.getenv("MAX_CONCURRENT_LLM", "2"))),
        min_interval_sec=max(0.0, float(os.getenv("LLM_REQUEST_DELAY", "1.1"))),
    )
    with _LIMITER_LOCK:
        if _LIMITER_INSTANCE is None or _LIMITER_CONFIG != config:
            _LIMITER_INSTANCE = LLMRateLimiter(
                max_concurrent=config.max_concurrent,
                min_interval_sec=config.min_interval_sec,
            )
            _LIMITER_CONFIG = config
    return _LIMITER_INSTANCE


def is_rate_limited_error(exc: Exception) -> bool:
    """Best-effort 429/rate-limit detection for OpenAI-compatible errors."""
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True
    message = str(exc).lower()
    return "429" in message or "rate limit" in message or "rate_limited" in message


def retry_after_seconds(exc: Exception) -> Optional[float]:
    """Extract Retry-After when available."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    value = str(headers.get("Retry-After", "")).strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def backoff_delay(attempt: int, exc: Optional[Exception] = None) -> float:
    """
    Exponential backoff with jitter.

    Priority:
    1) Retry-After (if present)
    2) exponential base * 2^attempt + jitter
    """
    retry_after = retry_after_seconds(exc) if exc is not None else None
    if retry_after is not None:
        return retry_after

    base = max(0.1, float(os.getenv("LLM_BACKOFF_BASE_SEC", "1.2")))
    jitter = max(0.0, float(os.getenv("LLM_BACKOFF_JITTER_SEC", "0.5")))
    cap = max(base, float(os.getenv("LLM_BACKOFF_MAX_SEC", "20")))

    delay = min(cap, base * (2 ** max(0, int(attempt))))
    if jitter > 0:
        delay += random.uniform(0.0, jitter)
    return min(delay, cap)
