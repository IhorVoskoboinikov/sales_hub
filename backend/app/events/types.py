"""Типы событий и команд в outbox_events.event_type (разделы 6 и 15).

Тип появляется вместе с потребителем и строкой в ROUTES (app/events/routes.py): команды
email.send и telegram.send — в шагах 5.3 и 5.12, доменные события record.assigned, deal.closed,
bulk_job.completed — в 5.12. До тех пор перечисление пустое: писать в outbox пока некому.
Тест test_every_event_type_has_route не даёт добавить тип без маршрута — такое событие relay не
смог бы опубликовать.
"""

from enum import StrEnum


class EventType(StrEnum):
    pass
