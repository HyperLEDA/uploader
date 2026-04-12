import http
from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from uploader.clients.client import call, parse_response
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi import errors, models, types


def _mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


def _response[T](
    parsed: T | None,
    *,
    status_code: http.HTTPStatus = http.HTTPStatus.OK,
) -> types.Response[T | models.HTTPValidationError]:
    return types.Response(
        status_code=status_code,
        content=b"",
        headers={},
        parsed=parsed,
    )


def test_parse_sync_detailed_response_ok() -> None:
    body = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    assert parse_response(_response(body)) is body


def test_parse_sync_detailed_response_validation_error() -> None:
    err = models.HTTPValidationError()
    with pytest.raises(RuntimeError):
        parse_response(_response(err))


def test_admin_sync_detailed_success_first_try() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())

    def method(**kwargs: Any) -> types.Response[Any]:
        assert kwargs["client"] is client
        assert kwargs["body"] is body
        return _response(ok)

    out = call(client, body, method)
    assert out is ok


def test_admin_sync_detailed_retries_on_5xx_response_then_succeeds() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    calls: list[int] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        calls.append(1)
        if len(calls) == 1:
            return _response(None, status_code=http.HTTPStatus.BAD_GATEWAY)
        return _response(ok)

    out = call(client, body, method, max_retries=5)
    assert out is ok
    assert len(calls) == 2


def test_admin_sync_detailed_retries_on_timeout_then_succeeds() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    calls: list[int] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow")
        return _response(ok)

    out = call(client, body, method, max_retries=5)
    assert out is ok
    assert len(calls) == 2


def test_admin_sync_detailed_retries_on_unexpected_status_5xx() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    calls: list[int] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        calls.append(1)
        if len(calls) == 1:
            raise errors.UnexpectedStatus(503, b"down")
        return _response(ok)

    out = call(client, body, method, max_retries=5)
    assert out is ok
    assert len(calls) == 2


def test_admin_sync_detailed_unexpected_status_4xx_no_retry() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])

    def method(**kwargs: Any) -> types.Response[Any]:
        raise errors.UnexpectedStatus(404, b"missing")

    with pytest.raises(errors.UnexpectedStatus):
        call(client, body, method, max_retries=5)


def test_admin_sync_detailed_exhausts_max_retries_timeout() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])

    def method(**kwargs: Any) -> types.Response[Any]:
        raise httpx.ReadTimeout("slow")

    with pytest.raises(httpx.ReadTimeout):
        call(client, body, method, max_retries=1)


def test_admin_sync_detailed_after_try_callback() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    seen: list[int] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        return _response(ok)

    def after_try(attempt: int) -> None:
        seen.append(attempt)

    call(client, body, method, after_try=after_try)
    assert seen == [1]


def test_call_callback_receives_message_string() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    seen: list[str] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        return _response(ok)

    call(client, body, method, callback_func=seen.append)
    assert len(seen) == 1
    assert "admin API request" in seen[0]
    assert "(attempt 1)" in seen[0]
    assert "method" in seen[0]


def test_call_callback_on_timeout_retry_sequence() -> None:
    client = _mock_client()
    body = models.AddDataRequest(table_name="t", data=[])
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    seen: list[str] = []
    calls: list[int] = []

    def method(**kwargs: Any) -> types.Response[Any]:
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow")
        return _response(ok)

    call(client, body, method, callback_func=seen.append, max_retries=5)
    assert len(seen) == 3
    assert "admin API request" in seen[0] and "(attempt 1)" in seen[0]
    assert "timeout" in seen[1] and "(attempt 1)" in seen[1]
    assert "admin API request" in seen[2] and "(attempt 2)" in seen[2]


def test_admin_sync_detailed_none_body_for_extra_kwargs() -> None:
    client = _mock_client()
    ok = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())

    def method(**kwargs: Any) -> types.Response[Any]:
        assert kwargs["client"] is client
        assert kwargs["table_name"] == "mytable"
        assert "body" not in kwargs
        return _response(ok)

    out = call(client, None, method, table_name="mytable")
    assert out is ok
