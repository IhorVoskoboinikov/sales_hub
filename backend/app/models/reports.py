"""Materialized view отчётов (раздел 6, «Отчёты»).

deal_stats — живые сделки, сгруппированные по организации, отделу, ответственному, воронке,
этапу, статусу и месяцу (для открытых — месяц создания, для закрытых — месяц закрытия, UTC).
Отчёт читает сотни строк вместо миллиона сделок; данные отстают на интервал обновления
(REFRESH MATERIALIZED VIEW CONCURRENTLY: python -m app.cli refresh-reports, с этапа 5 —
задача beat каждые 5 минут).

Описан на отдельной MetaData, а не в Base.metadata: Alembic не умеет сравнивать
materialized view и считал бы его пропавшей таблицей. Создаёт его миграция 602391a6401b.
"""

from sqlalchemy import BigInteger, Column, Date, MetaData, Numeric, String, Table, Uuid

reports_metadata = MetaData()

deal_stats = Table(
    "deal_stats",
    reports_metadata,
    Column("org_id", Uuid),
    Column("team_id", Uuid),
    Column("owner_id", Uuid),
    Column("pipeline_id", BigInteger),
    Column("stage_id", BigInteger),
    Column("status", String(8)),
    Column("month", Date),
    Column("deals", BigInteger),
    Column("amount_usd", Numeric(16, 2)),
)
