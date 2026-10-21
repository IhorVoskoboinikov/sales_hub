-- Генерация больших данных для замеров (раздел 3, «Генерация данных»; этап 4.8).
--
-- Запуск: make seed-big (psql в контейнере postgres). Пробный прогон на 1% объёма:
-- make seed-big SCALE=0.01. Повторный запуск отказывается — сначала make clean && make up.
--
-- 20 организаций, по 3–10 отделов, 2 000 пользователей (у всех пароль seed-password-1,
-- email вида user5@seed-01.example), по 1–2 воронки на 5 этапов, 300 тыс. компаний,
-- 5 млн контактов, 1 млн сделок, 20 млн активностей за 24 месяца, курсы НБУ за это время.
-- Распределение неравномерное: организации разного размера, а у первых менеджеров
-- организации записей в десятки раз больше, чем у последних, — как в жизни.
--
-- Как загрузить десятки миллионов строк за минуты, а не за час:
-- - всё в одной транзакции (кроме VACUUM): упало на середине — откатилось целиком;
-- - session_replication_role = replica: FK-триггеры не срабатывают (данные корректны по
--   построению — так же делает pg_restore --disable-triggers); CHECK-и работают;
-- - id не задаются явно — их выдаёт identity, поэтому последовательности не разъедутся;
-- - одна вставка на таблицу и на месяц активностей, строки месяца — по порядку created_at;
-- - GIN-индексы контактов и компаний удаляются перед загрузкой и строятся после: построить
--   индекс один раз намного быстрее, чем обновлять его на каждой из миллионов вставок;
-- - в конце VACUUM (ANALYZE): статистика для планировщика и visibility map для index-only scan.

\set ON_ERROR_STOP on
\timing on
\if :{?scale}
\else
  \set scale 1
\endif

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM organizations WHERE slug LIKE 'seed-%') THEN
        RAISE EXCEPTION 'seed-данные уже есть: make clean && make up && make seed-big';
    END IF;
END
$$;

-- Шаги 1–8 — одна транзакция: ошибка на середине откатывает всё, включая удаление
-- GIN-индексов, и скрипт можно просто запустить снова. VACUUM в транзакции нельзя — он после.
BEGIN;
SET LOCAL session_replication_role = replica;
SET LOCAL maintenance_work_mem = '1GB';
SET LOCAL synchronous_commit = off;  -- при сбое потеряется хвост загрузки, а не согласованность

-- Месяцы — по UTC, как границы партиций (::date взял бы часовой пояс сессии).
SELECT ensure_monthly_partitions(
    'activities',
    ((now() - interval '24 months') AT TIME ZONE 'UTC')::date,
    ((now() + interval '2 months') AT TIME ZONE 'UTC')::date
);

-- 1. Курсы НБУ за два года (гривен за единицу), с плавными колебаниями.
INSERT INTO fx_rates (rate_date, currency, rate_uah, source)
SELECT d::date, c.cur, round((c.base * (1 + 0.03 * sin(extract(epoch FROM d) / 2.6e6)))::numeric, 6),
       'SEED'
FROM generate_series(now()::date - 731, now()::date + 1, interval '1 day') d
CROSS JOIN (VALUES ('USD', 41.5), ('EUR', 45.2), ('PLN', 10.4)) c (cur, base)
ON CONFLICT DO NOTHING;

-- 2. Организации, отделы, люди. Временные таблицы — «справочник» генератора: номер
--    организации → её uuid, номер ответственного в организации → его id и отдел.
CREATE TEMP TABLE seed_orgs AS
SELECT g AS org_idx, gen_random_uuid() AS org_id, 3 + (g * 7) % 8 AS teams
FROM generate_series(1, 20) g;
INSERT INTO organizations (id, name, slug)
SELECT org_id, 'Seed ' || org_idx, 'seed-' || lpad(org_idx::text, 2, '0') FROM seed_orgs;

CREATE TEMP TABLE seed_teams AS
SELECT o.org_idx, t AS team_idx, gen_random_uuid() AS team_id
FROM seed_orgs o, generate_series(1, o.teams) t;
INSERT INTO teams (id, org_id, name)
SELECT t.team_id, o.org_id, 'Відділ ' || t.team_idx FROM seed_teams t JOIN seed_orgs o USING (org_idx);

-- В каждой организации 100 человек: 1 org_admin, 2 viewer, по руководителю на отдел,
-- остальные — менеджеры, разложенные по отделам.
CREATE TEMP TABLE seed_users AS
SELECT o.org_idx, o.org_id, n, gen_random_uuid() AS user_id,
       CASE WHEN n = 1 THEN 'org_admin' WHEN n <= 3 THEN 'viewer'
            WHEN n <= 3 + o.teams THEN 'team_lead' ELSE 'sales_rep' END AS role,
       CASE WHEN n <= 3 THEN NULL WHEN n <= 3 + o.teams THEN n - 3
            ELSE n % o.teams + 1 END AS team_idx
FROM seed_orgs o, generate_series(1, 100) n;
INSERT INTO users (id, email, password_hash, org_id, team_id, role, email_verified_at)
SELECT u.user_id, format('user%s@seed-%s.example', u.n, lpad(u.org_idx::text, 2, '0')),
       '$argon2id$v=19$m=65536,t=3,p=4$bXusr2/dSafPjDNpyAD3kA$RorHfTYlHZoRbMyzujqF5xxVtX8sHQh7o/SqbBVkdv8',
       u.org_id, t.team_id, u.role, now()
FROM seed_users u LEFT JOIN seed_teams t USING (org_idx, team_idx);

-- Ответственные (руководители и менеджеры), пронумерованные внутри организации. Номер
-- выбирается как 1 + floor(n * random()^4): первые номера получают в десятки раз больше.
-- Первыми идут менеджеры, руководители — в конце: «тяжёлый» ответственный должен быть
-- sales_rep — его список (уровень own) и проверяет make explain.
CREATE TEMP TABLE seed_owners AS
SELECT u.org_idx,
       row_number() OVER (PARTITION BY u.org_idx ORDER BY u.role = 'team_lead', u.n)::int AS rn,
       u.user_id, t.team_id
FROM seed_users u JOIN seed_teams t USING (org_idx, team_idx);
CREATE UNIQUE INDEX ON seed_owners (org_idx, rn);
CREATE TEMP TABLE seed_owner_counts AS SELECT org_idx, count(*)::int AS n FROM seed_owners GROUP BY 1;
ANALYZE seed_owners;

-- 3. Воронки: у каждой второй организации две, в каждой 5 этапов.
INSERT INTO pipelines (org_id, name)
SELECT org_id, 'Воронка ' || p FROM seed_orgs, generate_series(1, 1 + org_idx % 2) p;
INSERT INTO pipeline_stages (pipeline_id, name, position, probability)
SELECT p.id, s.name, s.pos, s.prob
FROM pipelines p JOIN seed_orgs o ON o.org_id = p.org_id
CROSS JOIN (VALUES ('Новий', 0, 10), ('Кваліфікація', 1, 25), ('Пропозиція', 2, 50),
                   ('Переговори', 3, 75), ('Договір', 4, 90)) s (name, pos, prob);
CREATE TEMP TABLE seed_stages AS
SELECT o.org_idx, row_number() OVER (PARTITION BY o.org_idx ORDER BY s.id)::int AS rn,
       s.id AS stage_id, s.pipeline_id
FROM pipeline_stages s JOIN pipelines p ON p.id = s.pipeline_id JOIN seed_orgs o ON o.org_id = p.org_id;
CREATE TEMP TABLE seed_stage_counts AS SELECT org_idx, count(*)::int AS n FROM seed_stages GROUP BY 1;

DROP INDEX ix_companies_name_trgm;
DROP INDEX ix_contacts_search_trgm;
DROP INDEX ix_contacts_tags;

-- 4. Компании. Организация — 1 + floor(20 * random()^2): первые организации крупнее.
--    Строки по порядку организации — тогда id компаний организации идут подряд, и
--    «случайная компания организации» — просто случайное число из её диапазона id.
INSERT INTO companies (org_id, team_id, owner_id, name, domain, industry, size, created_at, updated_at)
SELECT o.org_id, w.team_id, w.user_id, 'Компанія ' || x.g, 'company' || x.g || '.example',
       (ARRAY['IT', 'Торгівля', 'Виробництво', 'Логістика', 'Агро'])[1 + x.g % 5],
       5 + (x.g * 37) % 500, x.ts, x.ts
FROM (SELECT g, 1 + floor(20 * random() ^ 2)::int AS org_idx, random() AS r,
             now() - random() * interval '730 days' AS ts
      FROM generate_series(1, (300000 * :scale)::int) g) x
JOIN seed_orgs o USING (org_idx)
JOIN seed_owner_counts c USING (org_idx)
JOIN seed_owners w ON w.org_idx = x.org_idx AND w.rn = 1 + floor(c.n * x.r ^ 4)::int
ORDER BY x.org_idx;

CREATE TEMP TABLE seed_company_ranges AS
SELECT o.org_idx, min(c.id) AS lo, count(*) AS n
FROM companies c JOIN seed_orgs o ON o.org_id = c.org_id GROUP BY 1;

-- 5. Контакты: 70% привязаны к компании своей организации, 2% — в корзине.
INSERT INTO contacts (org_id, team_id, owner_id, company_id, first_name, last_name, email, phone,
                      status, source, tags, created_at, updated_at, deleted_at)
SELECT o.org_id, w.team_id, w.user_id,
       CASE WHEN x.r2 < 0.7 THEN cr.lo + floor(cr.n * random())::bigint END,
       (ARRAY['Іван', 'Олена', 'Петро', 'Марія', 'Андрій', 'Наталія', 'Олег', 'Ірина'])[1 + x.g % 8],
       (ARRAY['Петренко', 'Коваль', 'Шевченко', 'Бондар', 'Ткаченко', 'Мельник'])[1 + (x.g / 8) % 6]
           || ' ' || x.g,
       'c' || x.g || '@mail.example',
       '+38067' || lpad((x.g % 10000000)::text, 7, '0'),
       (ARRAY['lead', 'customer', 'churned'])[1 + floor(random() ^ 2 * 3)::int],
       (ARRAY['web', 'import', 'referral', 'cold'])[1 + x.g % 4],
       CASE WHEN x.r2 < 0.05 THEN '{vip,kyiv}'::varchar[] WHEN x.r2 < 0.15 THEN '{vip}'
            WHEN x.r2 < 0.35 THEN '{kyiv}' ELSE '{}' END,
       x.ts, x.ts,
       CASE WHEN random() < 0.02 THEN x.ts + interval '1 day' END
FROM (SELECT g, 1 + floor(20 * random() ^ 2)::int AS org_idx, random() AS r, random() AS r2,
             now() - random() * interval '730 days' AS ts
      FROM generate_series(1, (5000000 * :scale)::int) g) x
JOIN seed_orgs o USING (org_idx)
JOIN seed_owner_counts c USING (org_idx)
JOIN seed_owners w ON w.org_idx = x.org_idx AND w.rn = 1 + floor(c.n * x.r ^ 4)::int
JOIN seed_company_ranges cr ON cr.org_idx = x.org_idx
ORDER BY x.org_idx;

CREATE TEMP TABLE seed_contact_ranges AS
SELECT o.org_idx, min(c.id) AS lo, count(*) AS n
FROM contacts c JOIN seed_orgs o ON o.org_id = c.org_id GROUP BY 1;

-- 6. Сделки: 70% открыты, 20% выиграны, 10% проиграны; 80% введены в гривнах. Пересчёт в
--    USD — одним JOIN на fx_rates по дате создания, а не построчно в Python (раздел 3).
INSERT INTO deals (org_id, team_id, owner_id, contact_id, company_id, pipeline_id, stage_id, title,
                   amount_usd, amount_original, currency_original, fx_rate, fx_date, status,
                   expected_close_date, closed_at, created_at, updated_at, deleted_at)
SELECT o.org_id, w.team_id, w.user_id,
       ct.lo + floor(ct.n * random())::bigint,
       CASE WHEN random() < 0.5 THEN cr.lo + floor(cr.n * random())::bigint END,
       s.pipeline_id, s.stage_id, 'Угода ' || x.g,
       round(x.amount / CASE WHEN x.uah THEN fx.rate_uah ELSE 1 END, 2),
       x.amount, CASE WHEN x.uah THEN 'UAH' ELSE 'USD' END,
       CASE WHEN x.uah THEN fx.rate_uah ELSE 1 END, x.ts::date,
       x.status,
       (x.ts + (30 + floor(random() * 90)) * interval '1 day')::date,
       CASE WHEN x.status <> 'open'
            THEN least(now(), x.ts + random() * interval '60 days') END,
       x.ts, x.ts,
       CASE WHEN random() < 0.01 THEN now() END
FROM (SELECT g, 1 + floor(20 * random() ^ 2)::int AS org_idx, random() AS r, random() AS rs,
             random() < 0.8 AS uah,
             round((1000 + random() ^ 3 * 999000)::numeric, 2) AS amount,
             CASE WHEN random() < 0.7 THEN 'open' WHEN random() < 0.67 THEN 'won' ELSE 'lost' END
                 AS status,
             now() - random() * interval '730 days' AS ts
      FROM generate_series(1, (1000000 * :scale)::int) g) x
JOIN seed_orgs o USING (org_idx)
JOIN seed_owner_counts c USING (org_idx)
JOIN seed_owners w ON w.org_idx = x.org_idx AND w.rn = 1 + floor(c.n * x.r ^ 4)::int
JOIN seed_contact_ranges ct ON ct.org_idx = x.org_idx
JOIN seed_company_ranges cr ON cr.org_idx = x.org_idx
JOIN seed_stage_counts sc ON sc.org_idx = x.org_idx
JOIN seed_stages s ON s.org_idx = x.org_idx AND s.rn = 1 + floor(sc.n * x.rs)::int
JOIN fx_rates fx ON fx.rate_date = x.ts::date AND fx.currency = 'USD'
ORDER BY x.org_idx;

CREATE TEMP TABLE seed_deal_ranges AS
SELECT o.org_idx, min(d.id) AS lo, count(*) AS n
FROM deals d JOIN seed_orgs o ON o.org_id = d.org_id GROUP BY 1;
ANALYZE seed_orgs, seed_owner_counts, seed_contact_ranges, seed_deal_ranges;

-- 7. Активности: по отдельной вставке на месяц (каждая — в свою партицию), внутри месяца —
--    по порядку created_at, как они и копились бы в жизни. 5% — открытые задачи со сроком,
--    остальные уже состоялись (done_at = created_at).
SELECT format($sql$
INSERT INTO activities (org_id, team_id, owner_id, deal_id, contact_id, type, subject, due_at,
                        done_at, created_at)
SELECT o.org_id, w.team_id, w.user_id,
       CASE WHEN random() < 0.5 THEN dr.lo + floor(dr.n * random())::bigint END,
       CASE WHEN random() < 0.6 THEN ct.lo + floor(ct.n * random())::bigint END,
       x.type, 'Активність ' || x.g,
       CASE WHEN x.open OR x.type IN ('meeting', 'task')
            THEN x.ts + (1 + floor(random() * 14)) * interval '1 day' END,
       CASE WHEN NOT x.open THEN x.ts END,
       x.ts
FROM (SELECT g, 1 + floor(20 * random() ^ 2)::int AS org_idx, random() AS r,
             (ARRAY['call', 'meeting', 'email', 'task'])[1 + g %% 4] AS type,
             random() < 0.05 AS open,
             least(%1$L::timestamptz + random() * interval '1 month', now()) AS ts
      FROM generate_series(1, %2$s) g) x
JOIN seed_orgs o USING (org_idx)
JOIN seed_owner_counts c USING (org_idx)
JOIN seed_owners w ON w.org_idx = x.org_idx AND w.rn = 1 + floor(c.n * x.r ^ 4)::int
JOIN seed_contact_ranges ct ON ct.org_idx = x.org_idx
JOIN seed_deal_ranges dr ON dr.org_idx = x.org_idx
ORDER BY x.ts
$sql$, month, (20000000 * :scale / 24)::int)
FROM generate_series(date_trunc('month', now()) - interval '23 months', date_trunc('month', now()),
                     interval '1 month') month
\gexec

-- 8. Индексы, выключенные на время загрузки, — теми же определениями, что в миграциях.
CREATE INDEX ix_companies_name_trgm ON companies USING gin (name gin_trgm_ops)
    WHERE deleted_at IS NULL;
CREATE INDEX ix_contacts_search_trgm ON contacts USING gin ((lower((((first_name::text || ' '::text)
    || last_name::text) || ' '::text) || COALESCE(email::text, ''::text))) gin_trgm_ops)
    WHERE deleted_at IS NULL;
CREATE INDEX ix_contacts_tags ON contacts USING gin (tags) WHERE deleted_at IS NULL;

COMMIT;

-- 9. Статистика для планировщика, visibility map для index-only scan, отчёты.
VACUUM (ANALYZE) companies, contacts, deals, activities, users, teams, pipelines, pipeline_stages,
    fx_rates;
REFRESH MATERIALIZED VIEW deal_stats;

SELECT 'companies' AS t, count(*) FROM companies
UNION ALL SELECT 'contacts', count(*) FROM contacts
UNION ALL SELECT 'deals', count(*) FROM deals
UNION ALL SELECT 'activities', count(*) FROM activities;
