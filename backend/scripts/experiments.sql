-- Замеры индексов «без / с» на seed-данных (этап 4.9). Результаты и выводы — docs/experiments.md.
--
-- Запуск: make experiments (после make seed-big). Каждый эксперимент — в транзакции, которая
-- откатывается: DROP INDEX и CREATE INDEX в Postgres транзакционны, поэтому база после замера
-- та же. В рабочей базе так не делают (DROP INDEX держит эксклюзивную блокировку таблицы) —
-- там новый индекс создаётся CREATE INDEX CONCURRENTLY (раздел 9).

\set ON_ERROR_STOP on
\pset pager off

-- Кого и что измеряем: самая крупная seed-организация, её менеджер с наибольшим числом
-- клиентов, первая воронка, сделка с историей.
SELECT o.id AS org FROM organizations o JOIN contacts c ON c.org_id = o.id
WHERE o.slug LIKE 'seed-%' GROUP BY o.id ORDER BY count(*) DESC LIMIT 1 \gset
SELECT owner_id AS rep, count(*) AS rep_contacts FROM contacts
WHERE org_id = :'org' AND deleted_at IS NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 1 \gset
SELECT owner_id AS small_rep, count(*) AS small_contacts FROM contacts
WHERE org_id = :'org' AND deleted_at IS NULL GROUP BY 1 ORDER BY 2 LIMIT 1 \gset
SELECT id AS pipeline FROM pipelines WHERE org_id = :'org' ORDER BY id LIMIT 1 \gset
SELECT deal_id AS deal FROM activities WHERE org_id = :'org' AND deal_id IS NOT NULL
GROUP BY 1 ORDER BY count(*) DESC LIMIT 1 \gset
\echo org=:org rep=:rep (контактов: :rep_contacts) small_rep=:small_rep (контактов: :small_contacts)
\echo pipeline=:pipeline deal=:deal

\echo
\echo ======== E1. «Мои клиенты»: менеджер с наибольшим и с наименьшим числом клиентов
\echo --- с индексом (owner_id, created_at, id): большой менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
\echo --- с индексом (owner_id, created_at, id): маленький менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'small_rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
BEGIN;
DROP INDEX ix_contacts_owner_id_created_at;
CREATE INDEX ix_e1 ON contacts (org_id, owner_id) WHERE deleted_at IS NULL;
\echo --- с индексом (org_id, owner_id) из первой версии дизайна: большой менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
\echo --- с индексом (org_id, owner_id): маленький менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'small_rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
DROP INDEX ix_e1;
\echo --- без индекса по ответственному: большой менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
\echo --- без индекса по ответственному: маленький менеджер
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND owner_id = :'small_rep'
ORDER BY created_at DESC, id DESC LIMIT 51;
ROLLBACK;

\echo
\echo ======== E2. Глубокая страница: OFFSET против keyset (100 000-я строка организации)
SELECT created_at AS c, id AS i FROM contacts WHERE deleted_at IS NULL AND org_id = :'org'
ORDER BY created_at DESC, id DESC OFFSET 100000 LIMIT 1 \gset
\echo --- OFFSET 100000
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org'
ORDER BY created_at DESC, id DESC OFFSET 100000 LIMIT 51;
\echo --- keyset: (created_at, id) < курсор
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND (created_at, id) < (:'c', :'i')
ORDER BY created_at DESC, id DESC LIMIT 51;

\echo
\echo ======== E3. Поиск по подстроке (pg_trgm)
\echo --- редкая подстрока, с GIN-индексом
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org'
  AND lower((((first_name::text || ' '::text) || last_name::text) || ' '::text)
      || COALESCE(email::text, ''::text)) LIKE '%c4242@m%'
ORDER BY created_at DESC, id DESC LIMIT 51;
\echo --- частая подстрока (каждый шестой контакт), с GIN-индексом
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org'
  AND lower((((first_name::text || ' '::text) || last_name::text) || ' '::text)
      || COALESCE(email::text, ''::text)) LIKE '%петренко%'
ORDER BY created_at DESC, id DESC LIMIT 51;
BEGIN;
DROP INDEX ix_contacts_search_trgm;
\echo --- редкая подстрока, без индекса
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org'
  AND lower((((first_name::text || ' '::text) || last_name::text) || ' '::text)
      || COALESCE(email::text, ''::text)) LIKE '%c4242@m%'
ORDER BY created_at DESC, id DESC LIMIT 51;
ROLLBACK;

\echo
\echo ======== E4. Сегмент по тегу (массив, @>)
\echo --- с GIN (tags)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT count(*) FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND tags @> ARRAY['vip']::varchar[];
BEGIN;
DROP INDEX ix_contacts_tags;
\echo --- без индекса
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT count(*) FROM contacts
WHERE deleted_at IS NULL AND org_id = :'org' AND tags @> ARRAY['vip']::varchar[];
ROLLBACK;

\echo
\echo ======== E5. Канбан: покрывающий индекс и index-only scan
\echo --- с ix_deals_kanban (INCLUDE title, amount_usd, owner_id, team_id, org_id)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT stage_id, count(*), sum(amount_usd) FROM deals
WHERE pipeline_id = :pipeline AND status = 'open' AND deleted_at IS NULL AND org_id = :'org'
GROUP BY stage_id;
BEGIN;
DROP INDEX ix_deals_kanban;
\echo --- без него
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT stage_id, count(*), sum(amount_usd) FROM deals
WHERE pipeline_id = :pipeline AND status = 'open' AND deleted_at IS NULL AND org_id = :'org'
GROUP BY stage_id;
ROLLBACK;

\echo
\echo ======== E6. Просроченные открытые сделки организации
\echo --- с частичным индексом (org_id, expected_close_date, id) WHERE status = 'open'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM deals
WHERE deleted_at IS NULL AND org_id = :'org' AND status = 'open' AND expected_close_date < current_date
ORDER BY expected_close_date, id LIMIT 51;
BEGIN;
DROP INDEX ix_deals_overdue;
\echo --- без него
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM deals
WHERE deleted_at IS NULL AND org_id = :'org' AND status = 'open' AND expected_close_date < current_date
ORDER BY expected_close_date, id LIMIT 51;
ROLLBACK;

\echo
\echo ======== E7. История сделки на партиционированной таблице
\echo --- с индексом (deal_id, created_at, id) на каждой партиции
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM activities
WHERE deal_id = :deal ORDER BY created_at DESC, id DESC LIMIT 51;
SELECT created_at AS hc, id AS hi FROM activities WHERE deal_id = :deal
ORDER BY created_at DESC, id DESC OFFSET 0 LIMIT 1 \gset
\echo --- вторая страница: только сравнение строк — партиции не отсекаются
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM activities
WHERE deal_id = :deal AND (created_at, id) < (:'hc', :hi)
ORDER BY created_at DESC, id DESC LIMIT 51;
\echo --- вторая страница: плюс created_at <= курсор — более новые партиции отсечены
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM activities
WHERE deal_id = :deal AND (created_at, id) < (:'hc', :hi) AND created_at <= :'hc'
ORDER BY created_at DESC, id DESC LIMIT 51;
BEGIN;
DROP INDEX ix_activities_deal_id;
\echo --- без индекса (deal_id, …)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM activities
WHERE deal_id = :deal ORDER BY created_at DESC, id DESC LIMIT 51;
ROLLBACK;

\echo
\echo ======== E8. Открытые задачи менеджера: частичный индекс WHERE done_at IS NULL
\echo --- с частичным индексом (owner_id, due_at, id) WHERE done_at IS NULL
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM activities
WHERE done_at IS NULL AND org_id = :'org' AND owner_id = :'rep'
ORDER BY due_at, id LIMIT 51;
SELECT pg_size_pretty(sum(pg_relation_size(i.indexrelid))) AS partial_size
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname LIKE 'activities_p%owner_id_due_at_id_idx' \gset
BEGIN;
CREATE INDEX ix_e8 ON activities (owner_id, due_at, id);
SELECT pg_size_pretty(sum(pg_relation_size(i.indexrelid))) AS full_size
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname LIKE 'activities_p%owner_id_due_at_id_idx1' \gset
ROLLBACK;
\echo размер частичного индекса: :partial_size, такого же полного: :full_size

\echo
\echo ======== E9. BRIN: все активности за неделю (без фильтра по организации)
\echo --- без индекса по created_at (отсечение партиций уже оставило один месяц)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT count(*) FROM activities
WHERE created_at >= now() - interval '40 days' AND created_at < now() - interval '33 days';
BEGIN;
CREATE INDEX ix_e9 ON activities USING brin (created_at);
\echo --- с BRIN (created_at): строки месяца лежат на диске по порядку времени
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT count(*) FROM activities
WHERE created_at >= now() - interval '40 days' AND created_at < now() - interval '33 days';
SELECT pg_size_pretty(sum(pg_relation_size(i.indexrelid))) AS brin_size
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname LIKE 'activities_p%created_at_idx' \gset
SELECT pg_size_pretty(sum(pg_relation_size(i.indexrelid))) AS btree_size
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname LIKE 'activities_p%deal_id_created_at_id_idx' \gset
ROLLBACK;
\echo размер BRIN на 20 млн строк: :brin_size (для сравнения btree (deal_id, created_at, id): :btree_size)

\echo
\echo ======== E10. Отчёт по ответственным: materialized view против прямого запроса
\echo --- из deal_stats — тот же запрос, что строит ReportRepository.managers_query
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT s.owner_id, u.email,
       coalesce(sum(s.deals) FILTER (WHERE s.status = 'open'), 0),
       coalesce(sum(s.amount_usd) FILTER (WHERE s.status = 'open'), 0),
       coalesce(sum(s.deals) FILTER (WHERE s.status = 'won' AND s.month >= date_trunc('year', now())::date), 0),
       coalesce(sum(s.amount_usd) FILTER (WHERE s.status = 'won' AND s.month >= date_trunc('year', now())::date), 0),
       coalesce(sum(s.deals) FILTER (WHERE s.status = 'lost' AND s.month >= date_trunc('year', now())::date), 0),
       coalesce(sum(s.amount_usd) FILTER (WHERE s.status = 'lost' AND s.month >= date_trunc('year', now())::date), 0)
FROM deal_stats s JOIN users u ON u.id = s.owner_id
WHERE s.org_id = :'org'
GROUP BY s.owner_id, u.email ORDER BY u.email;
\echo --- то же прямо по deals
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT d.owner_id, u.email,
       count(*) FILTER (WHERE d.status = 'open'),
       coalesce(sum(d.amount_usd) FILTER (WHERE d.status = 'open'), 0),
       count(*) FILTER (WHERE d.status = 'won' AND d.closed_at >= date_trunc('year', now())),
       coalesce(sum(d.amount_usd) FILTER (WHERE d.status = 'won' AND d.closed_at >= date_trunc('year', now())), 0),
       count(*) FILTER (WHERE d.status = 'lost' AND d.closed_at >= date_trunc('year', now())),
       coalesce(sum(d.amount_usd) FILTER (WHERE d.status = 'lost' AND d.closed_at >= date_trunc('year', now())), 0)
FROM deals d JOIN users u ON u.id = d.owner_id
WHERE d.org_id = :'org' AND d.deleted_at IS NULL
GROUP BY d.owner_id, u.email ORDER BY u.email;
SELECT count(*) AS mv_rows FROM deal_stats WHERE org_id = :'org' \gset
SELECT count(*) AS org_deals FROM deals WHERE org_id = :'org' AND deleted_at IS NULL \gset
\echo строк агрегата организации: :mv_rows, её живых сделок: :org_deals
\echo --- сколько стоит обновить view
\timing on
REFRESH MATERIALIZED VIEW CONCURRENTLY deal_stats;
\timing off

\echo
\echo ======== E11. Дубль email: частичный UNIQUE (org_id, email) WHERE deleted_at IS NULL
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) SELECT id FROM contacts
WHERE org_id = :'org' AND email = 'c4242@mail.example' AND deleted_at IS NULL;
