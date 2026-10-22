"""Типы событий и команд в outbox_events.event_type (разделы 6 и 15).

Тип появляется вместе с потребителем и строкой в ROUTES (app/events/routes.py): команда
email.send — в шаге 5.3, telegram.send и доменные события record.assigned, deal.closed,
bulk_job.completed — в 5.12. Тест test_every_event_type_has_route не даёт добавить тип без
маршрута — такое событие relay не смог бы опубликовать.
"""

from enum import StrEnum


class EventType(StrEnum):
    # Команда, а не доменное событие: в контексте письма лежит сырой токен, поэтому на этапе 9
    # она не уходит в Kafka. Payload — OutboxRepository.add_email.
    EMAIL_SEND = "email.send"
