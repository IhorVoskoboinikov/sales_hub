"""Типы событий и команд в outbox_events.event_type (разделы 6 и 15) и схемы их payload.

Тип появляется вместе с потребителем и строкой в ROUTES (app/events/routes.py): команда
email.send — в шаге 5.3, telegram.send и доменные события record.assigned, deal.closed,
bulk_job.completed — в 5.12. Тест test_every_event_type_has_route не даёт добавить тип без
маршрута — такое событие relay не смог бы опубликовать.

Команды (email.send, telegram.send) — «отправь это этому человеку»: в них адрес, текст и секреты,
поэтому на этапе 9 они не уходят в Kafka. Доменные события — «что произошло»: кому и куда
писать, решает notifier (NotificationService.notify, раздел 16).
"""

from enum import StrEnum
from typing import TypedDict


class EventType(StrEnum):
    # Команда, а не доменное событие: в контексте письма лежит сырой токен, поэтому на этапе 9
    # она не уходит в Kafka. Payload — OutboxRepository.add_email.
    EMAIL_SEND = "email.send"
    # Команда: готовый текст одному пользователю. Payload — TelegramSend.
    TELEGRAM_SEND = "telegram.send"
    # Доменные события (раздел 16): их разбирает notifier.
    RECORD_ASSIGNED = "record.assigned"
    DEAL_CLOSED = "deal.closed"
    BULK_JOB_COMPLETED = "bulk_job.completed"


# id — строками: OutboxRepository.add переводит UUID и Decimal в строки (jsonb).


class TelegramSend(TypedDict):
    user_id: str
    text: str  # простой текст без parse_mode — экранировать нечего
    dedup_key: str


class RecordAssigned(TypedDict):
    """Контакт или сделка получили нового ответственного не от него самого (CrmService)."""

    entity_type: str  # contact / deal
    entity_id: int
    title: str  # имя контакта или название сделки — на момент события
    owner_id: str
    actor_id: str


class DealClosed(TypedDict):
    """Сделка перешла в won / lost через PATCH (DealService)."""

    deal_id: int
    title: str
    status: str  # won / lost
    amount_original: str
    currency_original: str
    owner_id: str
    team_id: str  # отдел ответственного: его руководители тоже получают сообщение
    actor_id: str


class BulkJobCompleted(TypedDict):
    """Финал bulk job — одна строка на job (BulkService._record_final)."""

    job_id: str
    kind: str
    status: str
    total: int
    processed: int
    failed_rows: int
    failed_chunks: int
    # Строки по сущностям — как result.rows ({contacts: n, …}); у импорта result другой
    # ({created, updated}), и здесь null.
    rows: dict[str, int] | None
    author_id: str
    to_user_id: str | None  # получатель reassign_owner: «вам передано N клієнтів»
