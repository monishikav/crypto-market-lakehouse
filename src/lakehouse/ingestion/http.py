"""Shared HTTP client with automatic retries.

Networks fail. A pipeline that dies on the first 503 or rate-limit (429) is not
production-grade. urllib3's Retry gives us exponential backoff (1s, 2s, 4s, ...) and
honours the server's Retry-After header, so transient errors heal themselves.
One Session per thread: connection pooling is fast, and nothing is shared across threads.
"""

from __future__ import annotations

import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "crypto-market-lakehouse/0.1 (learning project)"
_local = threading.local()


class NotFound(Exception):
    """The server says the file does not exist (HTTP 404) - not retryable."""


def _session() -> requests.Session:
    if not hasattr(_local, "session"):
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        s = requests.Session()
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.headers["User-Agent"] = USER_AGENT
        _local.session = s
    return _local.session


def http_get(url: str, headers: dict | None = None, timeout: int = 60) -> bytes:
    resp = _session().get(url, headers=headers, timeout=timeout)
    if resp.status_code == 404:
        raise NotFound(url)
    resp.raise_for_status()
    return resp.content
