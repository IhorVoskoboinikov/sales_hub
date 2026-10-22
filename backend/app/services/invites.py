"""Приглашения (разделы 4 и 5). Пользователь появляется в базе, только когда примет приглашение:
в токене лежат организация, отдел и роль, а пароль он задаёт сам.

Кто приглашает:
- супер-админ — первого админа новой организации (invite_first_admin);
- админ организации — людей в свою организацию с ролью и отделом (invite_member).

Письмо сервис не отправляет: пишет команду email.send в outbox в той же транзакции, что токен
и строка журнала. Отправит её воркер после коммита (notifications.send_email, раздел 4).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from app.authz.access import Access
from app.core.security import check_password_rules, hash_password
from app.errors import Conflict, NotFound, ValidationFailed
from app.models import ROLES_WITH_TEAM, EmailTokenPurpose, Organization, Role, User
from app.repositories import rate_limiter
from app.repositories.rate_limiter import RateLimiter
from app.services.email_tokens import format_expiry, issue_email_token, use_email_token
from app.texts import uk
from app.uow import UnitOfWork


@dataclass(frozen=True)
class Invite:
    email: str
    expires_at: datetime


class InviteService:
    def __init__(
        self,
        uow: UnitOfWork,
        limiter: RateLimiter,
        ttl: timedelta,
        docs_url: str,
    ) -> None:
        self.uow = uow
        self.limiter = limiter
        self.ttl = ttl
        self.docs_url = docs_url

    async def invite_first_admin(
        self, org_id: uuid.UUID, email: str, actor_id: uuid.UUID
    ) -> Invite:
        org = await self.uow.organizations.get(org_id)
        if org is None:
            raise NotFound(entity="organization", id=str(org_id))
        return await self._invite(org, email, Role.ORG_ADMIN, None, actor_id)

    async def invite_member(
        self, access: Access, email: str, role: Role, team_id: uuid.UUID | None
    ) -> Invite:
        """Правила (раздел 5): отдел обязателен для team_lead и sales_rep и не положен
        остальным; отдел — только своей организации."""
        if role in ROLES_WITH_TEAM and team_id is None:
            raise ValidationFailed("member.team_required", field="team_id")
        if role not in ROLES_WITH_TEAM and team_id is not None:
            raise ValidationFailed("member.team_not_allowed", field="team_id")
        if team_id is not None and await self.uow.teams.get(team_id, access.org_id) is None:
            raise ValidationFailed("member.team_not_found", field="team_id")
        org = await self.uow.organizations.get(access.org_id)
        if org is None:
            raise NotFound(entity="organization")
        return await self._invite(org, email, role, team_id, access.user_id)

    async def _invite(
        self,
        org: Organization,
        email: str,
        role: Role,
        team_id: uuid.UUID | None,
        actor_id: uuid.UUID,
    ) -> Invite:
        # 50 приглашений в час на организацию.
        await self.limiter.hit(rate_limiter.INVITE, str(org.id))
        expires_at = datetime.now(UTC) + self.ttl
        invite = {"role": role.value, "team_id": str(team_id) if team_id else None}
        existing = await self.uow.users.get_by_email(email)
        if existing is not None:
            # В журнал организации — то же, что при обычном приглашении: журнал читают её
            # админы (audit:read), и отдельная запись выдала бы, что email занят.
            self._audit(actor_id, org.id, "invite.created", email, invite)
            # Ответ тот же 202: админ чужой организации не должен узнать, что email занят.
            # А сам человек получит письмо и поймёт, что его пытались пригласить.
            self.uow.outbox.add_email(
                email,
                "already_member",
                {"org_name": org.name},
                org_id=org.id,
                user_id=existing.id,
            )
            await self.uow.commit()
            return Invite(email=email, expires_at=expires_at)
        token = await issue_email_token(
            self.uow,
            EmailTokenPurpose.INVITE,
            email,
            self.ttl,
            payload={"org_id": str(org.id), **invite},
        )
        self._audit(actor_id, org.id, "invite.created", email, invite)
        # Сырой токен — только в контексте письма (ключ token): после отправки его сотрут.
        self.uow.outbox.add_email(
            email,
            "invite",
            {
                "token": token.raw,
                "org_name": org.name,
                "role_title": uk.ROLE_TITLES[role],
                "docs_url": self.docs_url,
                "expires_at": format_expiry(token.row.expires_at),
            },
            org_id=org.id,
        )
        await self.uow.commit()
        return Invite(email=email, expires_at=token.row.expires_at)

    async def accept(self, raw_token: str, password: str) -> User:
        """Приглашённый задаёт пароль — и этим же подтверждает, что email его. В базе
        появляется пользователь с организацией, отделом и ролью из приглашения."""
        token = await use_email_token(self.uow, EmailTokenPurpose.INVITE, raw_token)
        payload = token.payload
        if not payload.get("org_id") or not payload.get("role"):
            raise ValidationFailed("email_token.invalid")  # приглашение старого формата
        org_id = uuid.UUID(payload["org_id"])
        team_id = uuid.UUID(payload["team_id"]) if payload.get("team_id") else None
        # Пока письмо шло, всё могло поменяться: email заняли, отдел или организацию удалили.
        stale = await self.uow.users.get_by_email(token.email) is not None or (
            await self.uow.organizations.get(org_id) is None
            # FOR KEY SHARE: отдел не удалят, пока не закоммитим (иначе 500 из-за FK).
            or (
                team_id is not None
                and await self.uow.teams.get(team_id, org_id, lock="key_share") is None
            )
        )
        if stale:
            raise Conflict("invite.stale")
        check_password_rules(password, token.email)
        now = datetime.now(UTC)
        user = User(
            id=uuid.uuid4(),  # id нужен до коммита — для записи в журнал
            email=token.email,
            password_hash=await hash_password(password),
            org_id=org_id,
            team_id=team_id,
            role=payload["role"],
            email_verified_at=now,
            created_at=now,  # заполняем сами: после коммита поле читается без запроса в базу
        )
        self.uow.users.add(user)
        self._audit(user.id, org_id, "member.joined", user.id, {"role": user.role}, "user")
        await self.uow.commit()
        return user

    def _audit(
        self,
        actor_id: uuid.UUID,
        org_id: uuid.UUID,
        action: str,
        entity_id: object,
        changes: dict[str, Any],
        entity_type: str = "invite",
    ) -> None:
        self.uow.audit.record(
            actor_id=actor_id,
            org_id=org_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            changes=changes,
        )
