"""Отправка в Telegram из воркера (раздел 16): один HTTP-вызов Bot API, без aiogram.

aiogram с его aiohttp-сессией нужен только процессу бота; здесь — общий httpx-клиент процесса
(его подменяет respx в тестах). Текст — простой, без parse_mode: названия сделок не нужно
экранировать.

Токен бота стоит в URL (…/bot<token>/sendMessage), поэтому он не должен попасть ни в лог, ни в
failed_tasks: логгеры httpx и httpcore — на уровне WARNING (app/core/logging.py), а клиент не
вызывает raise_for_status() (в тексте HTTPStatusError — URL) и бросает свои ошибки без URL.
Сетевые ошибки httpx переупаковываются без цепочки (from None): в traceback не попадает и
исходное исключение с запросом.
"""

from typing import Any

import httpx

TIMEOUT_SECONDS = 10.0


class TelegramApiError(Exception):
    """Bot API ответил ошибкой. status — HTTP-код, error_code и description — из тела ответа
    (их может не быть, если ответил не Telegram, а прокси). retry_after — из 429: столько секунд
    Telegram просит подождать (BaseTask берёт паузу ретрая отсюда)."""

    def __init__(
        self,
        status: int,
        error_code: int | None = None,
        description: str | None = None,
        retry_after: int | None = None,
    ) -> None:
        self.status = status
        self.error_code = error_code
        self.description = description
        self.retry_after = retry_after
        super().__init__(f"Telegram API {status}: {description or 'no description'}")

    @property
    def chat_unavailable(self) -> bool:
        """Писать в этот чат больше нельзя: пользователь заблокировал бота (403) или чата нет
        (400 «chat not found»). Привязка удаляется, ретрай бесполезен."""
        if self.status == 403:
            return True
        return self.status == 400 and "chat not found" in (self.description or "").lower()

    @property
    def retryable(self) -> bool:
        """429 — подождать retry_after; 5xx — сбой на стороне Telegram. Прочие 4xx — ошибка в
        запросе: повтор даст то же самое."""
        return self.status == 429 or self.status >= 500


class TelegramUnavailable(ConnectionError):
    """Сеть до Bot API: отказ, обрыв, таймаут. Наследник ConnectionError — BaseTask повторяет
    его как любую сетевую ошибку. В тексте — только класс исходной ошибки, без URL."""


class TelegramClient:
    def __init__(self, http: httpx.AsyncClient, token: str, base_url: str) -> None:
        self.http = http
        self._token = token
        self.base_url = base_url.rstrip("/")

    def __repr__(self) -> str:  # токен не печатается даже в отладке
        return f"TelegramClient(base_url={self.base_url!r})"

    async def send_message(self, chat_id: int, text: str) -> None:
        """sendMessage без превью ссылок. Успех — ничего; ошибка Bot API — TelegramApiError,
        сеть — TelegramUnavailable."""
        try:
            resp = await self.http.post(
                f"{self.base_url}/bot{self._token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "link_preview_options": {"is_disabled": True},
                },
                timeout=TIMEOUT_SECONDS,
            )
        except httpx.TransportError as exc:
            raise TelegramUnavailable(f"Telegram API unreachable: {type(exc).__name__}") from None
        body = _json(resp)
        if resp.is_success and body.get("ok") is True:
            return
        parameters = body.get("parameters")
        retry_after = parameters.get("retry_after") if isinstance(parameters, dict) else None
        error_code = body.get("error_code")
        description = body.get("description")
        raise TelegramApiError(
            resp.status_code,
            error_code if isinstance(error_code, int) else None,
            description if isinstance(description, str) else None,
            retry_after if isinstance(retry_after, int) else None,
        )


def _json(resp: httpx.Response) -> dict[str, Any]:
    """Тело ответа Bot API; не JSON (страница ошибки прокси) — пустой словарь."""
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}
