-- Удалить данные seed_big.sql (make seed-big-clean), не трогая остальные данные стенда.
-- Быстрее — make clean && make up: TRUNCATE томов вместо DELETE десятков миллионов строк.
\set ON_ERROR_STOP on
\timing on
BEGIN;
SET LOCAL session_replication_role = replica;  -- без FK-проверок на каждую удаляемую строку
CREATE TEMP TABLE seed_org_ids ON COMMIT DROP AS
    SELECT id FROM organizations WHERE slug LIKE 'seed-%';
-- Каскады FK при replica тоже выключены — поэтому зависимые строки удаляются явно.
DELETE FROM refresh_tokens
    WHERE user_id IN (SELECT id FROM users WHERE org_id IN (SELECT id FROM seed_org_ids));
DELETE FROM email_tokens
    WHERE user_id IN (SELECT id FROM users WHERE org_id IN (SELECT id FROM seed_org_ids));
DELETE FROM notes WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM audit_log WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM activities WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM deals WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM contacts WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM companies WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM pipeline_stages
    WHERE pipeline_id IN (SELECT id FROM pipelines WHERE org_id IN (SELECT id FROM seed_org_ids));
DELETE FROM pipelines WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM users WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM teams WHERE org_id IN (SELECT id FROM seed_org_ids);
DELETE FROM organizations WHERE id IN (SELECT id FROM seed_org_ids);
DELETE FROM fx_rates WHERE source = 'SEED';
COMMIT;
VACUUM (ANALYZE) companies, contacts, deals, activities;
REFRESH MATERIALIZED VIEW deal_stats;
