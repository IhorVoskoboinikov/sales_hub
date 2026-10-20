"""Все модели импортируются здесь — так Alembic видит их через Base.metadata.

Моделей пока нет: первые появятся на этапе 2 (users, refresh_tokens, email_tokens).
"""

from app.db.base import Base

__all__ = ["Base"]
