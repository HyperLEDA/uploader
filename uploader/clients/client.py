from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import httpx
import structlog

from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi import errors, models, types

logger = structlog.get_logger()


def parse_response[T](
    response: types.Response[T | models.HTTPValidationError],
) -> T:
    if isinstance(response.parsed, models.HTTPValidationError):
        raise RuntimeError(response)
    if response.parsed is None:
        raise RuntimeError(f"Unable to get response: {response.content}")

    return response.parsed


def _http_status_value(status: HTTPStatus | int) -> int:
    return int(status) if isinstance(status, HTTPStatus) else status


def _is_retryable_status(status: HTTPStatus | int) -> bool:
    return _http_status_value(status) >= 500


def _method_label(method: Callable[..., object]) -> str:
    qual = getattr(method, "__qualname__", None)
    if isinstance(qual, str):
        return qual
    return repr(method)


def call[T](
    client: adminapi.AuthenticatedClient,
    body: Any | None,
    method: Callable[..., types.Response[T | models.HTTPValidationError]],
    *,
    callback_func: Callable[[str], None] | None = None,
    after_try: Callable[[int], None] | None = None,
    max_retries: int | None = None,
    **kwargs: Any,
) -> T:
    call_kwargs: dict[str, Any] = {"client": client, **kwargs}
    if body is not None:
        call_kwargs["body"] = body

    label = _method_label(method)
    max_attempts = None if max_retries is None else max_retries + 1
    attempt = 0
    last_timeout: httpx.TimeoutException | None = None
    last_unexpected: errors.UnexpectedStatus | None = None
    last_retryable_response: types.Response[T | models.HTTPValidationError] | None = None

    while True:
        attempt += 1
        if max_attempts is not None and attempt > max_attempts:
            if last_timeout is not None:
                raise last_timeout
            if last_unexpected is not None:
                raise last_unexpected
            if last_retryable_response is not None:
                return parse_response(last_retryable_response)
            raise RuntimeError("admin API retries exhausted with no stored error")

        logger.info("admin API request", method=label, attempt=attempt)
        if callback_func is not None:
            callback_func(f"admin API request {label} (attempt {attempt})")

        try:
            response = method(**call_kwargs)
        except httpx.TimeoutException as exc:
            last_timeout = exc
            logger.warning(
                "admin API timeout, will retry",
                method=label,
                attempt=attempt,
                error=str(exc),
            )
            if callback_func is not None:
                callback_func(f"admin API timeout on {label} (attempt {attempt}), retrying: {exc}")
            if after_try is not None:
                after_try(attempt)
            continue
        except errors.UnexpectedStatus as exc:
            if not _is_retryable_status(exc.status_code):
                raise
            last_unexpected = exc
            logger.warning(
                "admin API server error, will retry",
                method=label,
                attempt=attempt,
                status_code=exc.status_code,
            )
            if callback_func is not None:
                callback_func(
                    f"admin API HTTP {exc.status_code} on {label} (attempt {attempt}), retrying",
                )
            if after_try is not None:
                after_try(attempt)
            continue

        if _is_retryable_status(response.status_code):
            last_retryable_response = response
            code = _http_status_value(response.status_code)
            logger.warning(
                "admin API server error response, will retry",
                method=label,
                attempt=attempt,
                status_code=code,
            )
            if callback_func is not None:
                callback_func(f"admin API HTTP {code} on {label} (attempt {attempt}), retrying")
            if after_try is not None:
                after_try(attempt)
            continue

        if after_try is not None:
            after_try(attempt)
        return parse_response(response)
