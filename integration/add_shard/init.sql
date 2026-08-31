-- Reset all three databases used by this suite.
--
-- Everything is schema-qualified: the connecting role is "pgdog" and
-- pgdog installs an internal schema with the same name, so unqualified
-- names would resolve to "pgdog"."..." via the default search_path and
-- survive a reset of schema public.
\c pgdog1
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%';
DROP PUBLICATION IF EXISTS __pgdog_suite;
DROP TABLE IF EXISTS public.orgs, public.data, public.packages, pgdog.orgs, pgdog.data CASCADE;
CREATE TABLE public.orgs (id VARCHAR PRIMARY KEY, shard_id BIGINT NOT NULL);
CREATE TABLE public.data (id BIGSERIAL, org_id VARCHAR NOT NULL, value TEXT, PRIMARY KEY (id, org_id));
-- Hybrid (broadcast_null) table: org_id NULL rows exist on every
-- shard; ids are app-supplied so broadcast rows stay identical.
CREATE TABLE public.packages (id BIGINT PRIMARY KEY, org_id VARCHAR, value TEXT);

\c pgdog2
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%';
DROP TABLE IF EXISTS public.orgs, public.data, public.packages, pgdog.orgs, pgdog.data CASCADE;
CREATE TABLE public.orgs (id VARCHAR PRIMARY KEY, shard_id BIGINT NOT NULL);
CREATE TABLE public.data (id BIGSERIAL, org_id VARCHAR NOT NULL, value TEXT, PRIMARY KEY (id, org_id));
CREATE TABLE public.packages (id BIGINT PRIMARY KEY, org_id VARCHAR, value TEXT);

-- The new shard starts completely empty: every non-system schema goes,
-- including pgdog's internal ones (the restore and setup recreate them)
-- and any leftovers from other test suites.
\c shard_0
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%';
DO $$
DECLARE s TEXT;
BEGIN
    -- The underscore is escaped: a bare 'pg_%' is 'pg' plus any
    -- character, which would spare schemas like pgdog_schema_test.
    FOR s IN SELECT nspname FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public')
        AND nspname NOT LIKE 'pg\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP SCHEMA %I CASCADE', s);
    END LOOP;
END $$;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
