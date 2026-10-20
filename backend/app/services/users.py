from datetime import UTC, datetime

from app.core.security import check_password_rules, hash_password
from app.models import User
from app.uow import UnitOfWork


async def create_superadmin(uow: UnitOfWork, email: str, password: str) -> tuple[User, bool]:
    """Создать супер-админа с подтверждённым email. Идемпотентно: если пользователь с таким
    email уже есть, ничего не меняет. Возвращает пользователя и признак «создан сейчас»."""
    existing = await uow.users.get_by_email(email)
    if existing is not None:
        return existing, False
    check_password_rules(password, email)
    user = User(
        email=email,
        password_hash=await hash_password(password),
        is_superadmin=True,
        email_verified_at=datetime.now(UTC),
    )
    uow.users.add(user)
    await uow.commit()
    return user, True
