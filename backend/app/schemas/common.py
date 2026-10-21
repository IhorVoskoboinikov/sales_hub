"""Общие схемы ответов: страница списка."""

from typing import Annotated

from fastapi import Path
from pydantic import BaseModel, Field

# Id CRM-записей — bigint. Без верхней границы число больше 2^63 - 1 дошло бы до asyncpg и
# стало 500, а не 422.
MAX_ID = 2**63 - 1
RecordId = Annotated[int, Path(ge=1, le=MAX_ID)]  # id в пути: /contacts/{contact_id}


class PageOut[T](BaseModel):
    """Страница списка с keyset-пагинацией. Следующая — тот же запрос с ?cursor=next_cursor."""

    items: list[T]
    next_cursor: str | None = Field(
        description="Курсор следующей страницы; null — это последняя страница"
    )
