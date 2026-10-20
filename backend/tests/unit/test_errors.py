"""Единый формат ошибок: AppError, валидация, ошибки Starlette, неожиданные исключения."""

from typing import Literal

import pytest
from fastapi import APIRouter, FastAPI, Query
from httpx import AsyncClient
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from structlog.testing import capture_logs

from app.errors import AppError, Conflict, NotFound, RateLimited, Unauthorized
from app.texts import uk

router = APIRouter(prefix="/test")


class Payload(BaseModel):
    email: str
    password: str = Field(min_length=8)


class TokenExpired(Unauthorized):
    code = "auth.test_without_text"  # кода нет в uk.ERRORS


class FakeDbError(Exception):
    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate


@router.get("/not-found")
async def raise_not_found() -> None:
    raise NotFound(entity="deal", id=123)


@router.get("/last-admin")
async def raise_conflict_with_own_code() -> None:
    raise Conflict("member.last_admin")


@router.get("/no-text")
async def raise_subclass_without_text() -> None:
    raise TokenExpired()


@router.get("/sort")
async def sort_items(order: Literal["asc", "desc"]) -> dict[str, str]:
    return {"order": order}


@router.get("/boom")
async def raise_unexpected() -> None:
    raise RuntimeError("секретная деталь")


@router.get("/items/{item_id}")
async def get_item(item_id: int, limit: int = Query(ge=1)) -> dict[str, int]:
    return {"item_id": item_id, "limit": limit}


@router.post("/body")
async def post_body(payload: Payload) -> dict[str, str]:
    return {"email": payload.email}


@router.get("/integrity/{sqlstate}")
async def raise_integrity(sqlstate: str) -> None:
    raise IntegrityError("INSERT ...", None, FakeDbError(sqlstate))


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(router)
    return app


async def test_app_error_has_common_format(client: AsyncClient) -> None:
    resp = await client.get("/test/not-found")

    assert resp.status_code == 404
    assert resp.headers["content-type"] == "application/problem+json"
    assert resp.json() == {
        "type": "https://saleshub.local/errors/not_found",
        "code": "not_found",
        "title": "Не знайдено",
        "status": 404,
        "request_id": resp.headers["x-request-id"],
        "details": {"entity": "deal", "id": 123},
    }


async def test_own_code_keeps_class_title(client: AsyncClient) -> None:
    resp = await client.get("/test/last-admin")

    assert resp.status_code == 409
    assert resp.json()["code"] == "member.last_admin"
    assert resp.json()["title"] == "Конфлікт даних"


async def test_subclass_without_text_keeps_its_status(client: AsyncClient) -> None:
    resp = await client.get("/test/no-text")

    assert resp.status_code == 401
    assert resp.json()["code"] == "auth.test_without_text"
    assert resp.json()["title"] == "Недійсний токен"  # текст родительского класса


def all_subclasses(cls: type[AppError]) -> list[type[AppError]]:
    return [sub for direct in cls.__subclasses__() for sub in [direct, *all_subclasses(direct)]]


def test_every_error_class_has_ukrainian_title() -> None:
    classes = [AppError, *all_subclasses(AppError)]
    # Тестовый класс выше специально без текста — его не считаем.
    missing = [c.__name__ for c in classes if c.code not in uk.ERRORS and c is not TokenExpired]

    assert missing == [], f"добавьте тексты в app/texts/uk.py: {missing}"


async def test_unexpected_error_is_500_without_details(client: AsyncClient) -> None:
    with capture_logs() as logs:
        resp = await client.get("/test/boom")

    assert resp.status_code == 500
    body = resp.json()
    assert body["code"] == "internal"
    assert body["request_id"] == resp.headers["x-request-id"]
    assert "секретная деталь" not in resp.text
    # traceback — только в логе
    assert any(e["event"] == "unhandled_error" and e.get("exc_info") for e in logs)
    access = next(e for e in logs if e["event"] == "request")
    assert access["status"] == 500


async def test_validation_messages_are_ukrainian(client: AsyncClient) -> None:
    resp = await client.get("/test/items/abc", params={"limit": 0})

    assert resp.status_code == 422
    assert resp.json()["code"] == "validation"
    errors = {tuple(e["loc"]): e["message"] for e in resp.json()["details"]["errors"]}
    assert errors == {
        ("path", "item_id"): "Має бути цілим числом",
        ("query", "limit"): "Має бути не менше 1",
    }


async def test_choice_message_has_no_english(client: AsyncClient) -> None:
    resp = await client.get("/test/sort", params={"order": "up"})

    assert resp.json()["details"]["errors"][0]["message"] == "Допустимі значення: 'asc' або 'desc'"


async def test_validation_error_does_not_echo_input(client: AsyncClient) -> None:
    resp = await client.post("/test/body", json={"email": "a@b.c", "password": "s3cr"})

    assert resp.status_code == 422
    assert resp.json()["details"]["errors"][0]["message"] == "Мінімальна довжина — 8"
    assert "s3cr" not in resp.text


async def test_unknown_route_is_404_in_common_format(client: AsyncClient) -> None:
    resp = await client.get("/no-such-route")

    assert resp.status_code == 404
    assert resp.json()["code"] == "not_found"


async def test_wrong_method_is_405_with_allow_header(client: AsyncClient) -> None:
    resp = await client.post("/health/live")

    assert resp.status_code == 405
    assert resp.json()["code"] == "method_not_allowed"
    assert resp.headers["allow"] == "GET"


async def test_unique_violation_is_409(client: AsyncClient) -> None:
    resp = await client.get("/test/integrity/23505")

    assert resp.status_code == 409
    assert resp.json()["code"] == "conflict"


async def test_other_integrity_error_is_500(client: AsyncClient) -> None:
    resp = await client.get("/test/integrity/23503")  # нарушение внешнего ключа

    assert resp.status_code == 500
    assert resp.json()["code"] == "internal"


def test_rate_limited_sets_retry_after_header() -> None:
    assert RateLimited(retry_after=42).response_headers() == {"Retry-After": "42"}
    assert RateLimited().response_headers() is None
