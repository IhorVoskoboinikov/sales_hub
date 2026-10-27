"""Уведомления из воркера (разделы 15 и 16): notifier и отправители команд из outbox.

- notify(event_id) — доменное событие (record.assigned, deal.closed, bulk_job.completed) →
  кому и в какой канал: команды email.send и telegram.send в outbox, одним коммитом.
- send_email(event_id) — команда email.send → SMTP.
- send_telegram(event_id) — команда telegram.send → Bot API.

Задачи получают только id события: payload с адресом, шаблоном и контекстом (у писем сервисов в
нём сырой токен) читается из базы, в Redis его нет. Отправка «как минимум один раз», дубли гасит
sent_notifications: ключ занимается в той же транзакции, что и отправка. Повтор notify пишет
команды с теми же ключами дедупа — отправители их пропустят.
"""

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import structlog

from app.emails.render import render_email, render_text
from app.emails.sender import EmailSender
from app.events.types import EventType
from app.models import NotificationChannel, OutboxEvent, User
from app.notifications import catalog
from app.notifications.catalog import NotificationEvent
from app.notifications.keys import email_key, event_subject, job_subject, notify_key
from app.notifications.telegram import TelegramApiError, TelegramClient
from app.texts import uk
from app.uow import UnitOfWork

logger = structlog.get_logger("notifications")


@dataclass(frozen=True)
class Message:
    """Одно уведомление одному человеку — до выбора каналов. setting — по какой настройке
    решать каналы: у «вам передано N клієнтів» это record.assigned получателя, а не
    bulk_job.completed. context — только строки, готовые для шаблона."""

    user_id: uuid.UUID
    setting: NotificationEvent
    template: str
    context: dict[str, str]


def reachable(user: User | None, org_id: uuid.UUID | None) -> bool:
    """Писать можно активному пользователю своей организации. Отключённому и пользователю без
    организации (супер-админ) — нет."""
    return user is not None and user.is_active and user.org_id is not None and user.org_id == org_id


def rows_text(rows: dict[str, int] | None) -> str:
    """Итог job по сущностям для шаблона: «контакти — 100, компанії — 20». Нулевые не
    показываем; нечего показать — пустая строка (шаблон пропустит строку)."""
    if not rows:
        return ""
    # Порядок — словаря NOTIFY_BULK_ROWS: jsonb хранит ключи в своём порядке, не в нашем.
    order = list(uk.NOTIFY_BULK_ROWS)
    names = sorted(rows, key=lambda name: order.index(name) if name in order else len(order))
    return ", ".join(
        f"{uk.NOTIFY_BULK_ROWS.get(name, name)} — {rows[name]}" for name in names if rows[name]
    )


class NotificationService:
    def __init__(
        self,
        uow: UnitOfWork,
        email_sender: EmailSender | None = None,
        telegram: TelegramClient | None = None,
    ) -> None:
        """email_sender и telegram — только у воркера (Resources, for_worker=True); telegram —
        None и без токена бота: тогда send_telegram сообщения пропускает. notify ни то, ни
        другое не нужно — он только пишет команды."""
        self.uow = uow
        self.email_sender = email_sender
        self.telegram = telegram

    # --- notifier (раздел 16, «Как сообщение доходит до пользователя», шаг 2)

    async def notify(self, event_id: int) -> int:
        """Разобрать доменное событие: получатели × каналы → команды в outbox, один коммит.
        Возвращает число записанных команд.

        Получатели: у record.assigned и deal.closed автор изменения (actor_id) исключается —
        себе не пишем; у bulk_job.completed автор job — законный получатель итога. Всегда
        исключаются неактивные и пользователи без организации, повторы схлопываются (получатель
        передачи, он же автор, получает одно сообщение — итог). Канал — по итоговой настройке
        пары; Telegram — только тем, у кого есть telegram_links."""
        try:
            event = await self.uow.outbox.get(event_id)
            if event is None:
                # Строку удалила очистка (опубликованные старше 7 дней) — разбирать нечего.
                logger.warning("notify_event_missing", event_id=event_id)
                return 0
            messages, users = await self._messages(event)
            written = await self._write_commands(event, messages, users)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.info("notify_done", event_id=event_id, event_type=event.event_type, commands=written)
        return written

    async def _messages(self, event: OutboxEvent) -> tuple[list[Message], dict[uuid.UUID, User]]:
        payload = event.payload
        if event.event_type == EventType.RECORD_ASSIGNED:
            return await self._record_assigned(payload)
        if event.event_type == EventType.DEAL_CLOSED:
            return await self._deal_closed(event.org_id, payload)
        if event.event_type == EventType.BULK_JOB_COMPLETED:
            return await self._bulk_job_completed(payload)
        raise ValueError(f"notifier не разбирает событие {event.event_type}")

    async def _record_assigned(
        self, payload: dict[str, Any]
    ) -> tuple[list[Message], dict[uuid.UUID, User]]:
        owner_id, actor_id = uuid.UUID(payload["owner_id"]), uuid.UUID(payload["actor_id"])
        users = await self._users([owner_id, actor_id])
        context = {
            "entity": uk.NOTIFY_ENTITIES.get(payload["entity_type"], payload["entity_type"]),
            "title": payload["title"],
            "actor": self._name(users, actor_id),
        }
        message = Message(owner_id, NotificationEvent.RECORD_ASSIGNED, "record_assigned", context)
        return self._without(actor_id, [message]), users

    async def _deal_closed(
        self, org_id: uuid.UUID | None, payload: dict[str, Any]
    ) -> tuple[list[Message], dict[uuid.UUID, User]]:
        owner_id, actor_id = uuid.UUID(payload["owner_id"]), uuid.UUID(payload["actor_id"])
        users = await self._users([owner_id, actor_id])
        # Руководители отдела ответственного — того, что записан в событии (отдел сделки).
        leads = (
            await self.uow.users.active_team_leads(org_id, uuid.UUID(payload["team_id"]))
            if org_id is not None
            else []
        )
        users.update({lead.id: lead for lead in leads})
        context = {
            "title": payload["title"],
            "result": uk.NOTIFY_DEAL_RESULTS.get(payload["status"], payload["status"]),
            "amount": f"{payload['amount_original']} {payload['currency_original']}",
            "owner": self._name(users, owner_id),
            "actor": self._name(users, actor_id),
        }
        messages = [
            Message(user_id, NotificationEvent.DEAL_CLOSED, "deal_closed", context)
            for user_id in [owner_id, *(lead.id for lead in leads)]
        ]
        return self._without(actor_id, messages), users

    async def _bulk_job_completed(
        self, payload: dict[str, Any]
    ) -> tuple[list[Message], dict[uuid.UUID, User]]:
        author_id = uuid.UUID(payload["author_id"])
        to_user_id = uuid.UUID(payload["to_user_id"]) if payload.get("to_user_id") else None
        users = await self._users([author_id, *([to_user_id] if to_user_id else [])])
        rows: dict[str, int] | None = payload.get("rows")
        summary = {
            "operation": uk.NOTIFY_BULK_KINDS.get(payload["kind"], payload["kind"]),
            "status": uk.NOTIFY_BULK_STATUSES.get(payload["status"], payload["status"]),
            "processed": str(payload["processed"]),
            "total": str(payload["total"]),
            "failed_rows": str(payload["failed_rows"]),
            "failed_chunks": str(payload["failed_chunks"]),
            "rows": rows_text(rows),
        }
        messages = [
            Message(author_id, NotificationEvent.BULK_JOB_COMPLETED, "bulk_job_completed", summary)
        ]
        # Получателю передачи — «вам передано N клієнтів» по его настройке record.assigned.
        # Ничего не передано (отменили до первого чанка, всё упало) — не о чем сообщать.
        transferred = sum(n for name, n in (rows or {}).items() if name != "failed")
        if to_user_id is not None and transferred > 0:
            context = {
                "count": str(transferred),
                "rows": rows_text(rows),
                "author": self._name(users, author_id),
            }
            messages.append(
                Message(to_user_id, NotificationEvent.RECORD_ASSIGNED, "bulk_reassigned", context)
            )
        # Автор, он же получатель, — одно сообщение с итогом (первое в списке).
        return self._without(None, messages), users

    async def _users(self, user_ids: Iterable[uuid.UUID]) -> dict[uuid.UUID, User]:
        return {user.id: user for user in await self.uow.users.get_many(set(user_ids))}

    @staticmethod
    def _name(users: dict[uuid.UUID, User], user_id: uuid.UUID) -> str:
        # Имён у пользователей нет — показываем email (пользователя могли удалить).
        user = users.get(user_id)
        return user.email if user is not None else uk.NOTIFY_UNKNOWN_USER

    @staticmethod
    def _without(actor_id: uuid.UUID | None, messages: list[Message]) -> list[Message]:
        """Без автора изменения (себе не пишем) и без повторов: первое сообщение человеку
        остаётся, следующие ему же — отбрасываются."""
        seen: set[uuid.UUID] = set()
        result = []
        for message in messages:
            if message.user_id == actor_id or message.user_id in seen:
                continue
            seen.add(message.user_id)
            result.append(message)
        return result

    async def _write_commands(
        self, event: OutboxEvent, messages: list[Message], users: dict[uuid.UUID, User]
    ) -> int:
        """Для каждой пары «получатель × канал» с включённой итоговой настройкой — команда в
        outbox. Ключи дедупа детерминированы: повтор notify даст те же."""
        recipients = [m for m in messages if reachable(users.get(m.user_id), event.org_id)]
        ids = [m.user_id for m in recipients]
        overrides = await self.uow.notification_settings.for_users(ids)
        linked = await self.uow.telegram_links.linked(ids)
        if event.event_type == EventType.BULK_JOB_COMPLETED:
            subject = job_subject(event.payload["job_id"])
        else:
            subject = event_subject(event.id)
        written = 0
        for message in recipients:
            user = users[message.user_id]
            for channel in catalog.channels(message.setting):
                if not catalog.enabled(overrides.get(user.id, {}), message.setting, channel):
                    continue
                key = notify_key(event.event_type, subject, user.id, channel)
                if channel == NotificationChannel.TELEGRAM:
                    if user.id not in linked:
                        continue  # Telegram не привязан — канала нет
                    text = render_text(message.template, **message.context)
                    self.uow.outbox.add_telegram(user.id, text, key, org_id=event.org_id)
                else:
                    self.uow.outbox.add_email(
                        user.email,
                        message.template,
                        message.context,
                        org_id=event.org_id,
                        user_id=user.id,
                        dedup_key=key,
                    )
                written += 1
        return written

    # --- отправители команд

    async def send_email(self, event_id: int) -> None:
        """Отправить письмо команды email.send одной транзакцией (раздел 15):

        занять ключ дедупа → отрисовать шаблон → SMTP → стереть токен из payload → коммит.
        Ключ уже занят — письмо уже ушло, выход. Ошибка — откат: ключ освобождается, токен
        остаётся в payload, и ретрай (или перезапуск из failed_tasks) отправит письмо снова.
        Дубль возможен, только если процесс упадёт между ответом SMTP и коммитом.

        Письмо notifier (в команде есть user_id и dedup_key) получателю, которого после notify
        отключили или убрали из организации события, не отправляется: ретрай или перезапуск из
        failed_tasks через дни не должен слать ему данные CRM. Ключ при этом не занимается.
        Письма сервисов (приглашение, сброс пароля — без dedup_key) так не проверяются: их
        получатель решён сервисом, а у приглашения пользователя ещё нет.

        Таймаут — на каждую команду SMTP (SmtpEmailSender, 10 с), а не asyncio.timeout на всю
        отправку: тот прервал бы и QUIT после принятого DATA — письмо ушло, а откат и ретрай
        отправили бы его снова. Ошибки QUIT aiosmtplib игнорирует сам; от медленного сервера
        воркер страхует task_soft_time_limit."""
        if self.email_sender is None:  # Resources собраны не для воркера — ошибка в коде
            raise RuntimeError("email_sender is not configured: Resources.create(for_worker=True)")
        try:
            event = await self.uow.outbox.get(event_id)
            if event is None:
                # Строку удалила очистка (опубликованные старше 7 дней) — отправлять нечего.
                logger.warning("email_event_missing", event_id=event_id)
                return
            payload = event.payload
            key = payload.get("dedup_key") or email_key(event_id)
            user_id = uuid.UUID(payload["user_id"]) if payload.get("user_id") else None
            if payload.get("dedup_key") and user_id is not None:
                recipient = await self.uow.users.get(user_id)
                if not reachable(recipient, event.org_id):
                    logger.info("email_recipient_inactive", event_id=event_id, user_id=str(user_id))
                    await self.uow.rollback()
                    return
            if not await self.uow.sent_notifications.claim(key, NotificationChannel.EMAIL, user_id):
                logger.info("email_duplicate", event_id=event_id, dedup_key=key)
                await self.uow.rollback()
                return
            email = render_email(payload["template"], payload["to"], **payload["context"])
            await self.email_sender.send(email)
            await self.uow.outbox.scrub_token(event_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise

    async def send_telegram(self, event_id: int) -> None:
        """Отправить сообщение команды telegram.send (раздел 16, шаг 5): та же схема, что у
        письма, — ключ дедупа → sendMessage (таймаут 10 с) → коммит; ошибка — откат и ретрай.

        Чат — из telegram_links на момент отправки: отвязали после notify — не отправляем.
        Telegram ответил 403 (бот заблокирован) или 400 «chat not found» — откат (ключ
        свободен), удалить именно прочитанную привязку (user_id, chat_id, linked_at) — свежую
        перепривязку, даже к тому же чату, это не задевает — и коммит, без ретрая: канал
        выключен, пользователь включит его через /start.
        429, 5xx и сеть — исключение наружу, BaseTask повторит (429 — через retry_after + 1 с);
        прочие 4xx — провал в failed_tasks. Бот не настроен (нет токена) — пропуск."""
        try:
            event = await self.uow.outbox.get(event_id)
            if event is None:
                logger.warning("telegram_event_missing", event_id=event_id)
                return
            if self.telegram is None:
                logger.info("telegram_skipped", event_id=event_id, reason="bot_not_configured")
                return
            payload = event.payload
            key = payload["dedup_key"]
            user_id = uuid.UUID(payload["user_id"])
            link = await self.uow.telegram_links.get(user_id)
            if link is None:
                logger.info("telegram_not_linked", event_id=event_id, user_id=str(user_id))
                await self.uow.rollback()
                return
            # До отката: после него объекты сессии не читаются.
            chat_id, linked_at = link.chat_id, link.linked_at
            claimed = await self.uow.sent_notifications.claim(
                key, NotificationChannel.TELEGRAM, user_id
            )
            if not claimed:
                logger.info("telegram_duplicate", event_id=event_id, dedup_key=key)
                await self.uow.rollback()
                return
            try:
                await self.telegram.send_message(chat_id, payload["text"])
            except TelegramApiError as exc:
                if not exc.chat_unavailable:
                    raise
                await self.uow.rollback()
                await self.uow.telegram_links.delete(user_id, chat_id, linked_at)
                await self.uow.commit()
                logger.warning(
                    "telegram_chat_unavailable",
                    event_id=event_id,
                    user_id=str(user_id),
                    status=exc.status,
                )
                return
            await self.uow.commit()
            # Текст сообщения в лог не пишем — только кому и по какому событию.
            logger.info("telegram_sent", event_id=event_id, user_id=str(user_id))
        except BaseException:
            await self.uow.rollback()
            raise
