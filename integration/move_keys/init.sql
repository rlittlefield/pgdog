-- Reset both shards used by this suite.
--
-- The suite's sharding rule is column-only (any table bearing org_id
-- is sharded), so leftovers from other suites with an org_id column
-- would join the move: wipe every non-system schema, like the
-- add_shard suite does for its empty shard, and rebuild public.
--
-- The data table's primary key includes org_id and its ids are
-- globally unique (interleaved sequences: odd on shard 0, even on
-- shard 1), the documented prerequisite for moving rows between
-- shards. The composite key also puts the sharding column in the
-- replica identity, which MOVE KEYS requires.
\c pgdog1
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%';
DO $$
DECLARE s TEXT;
BEGIN
    -- The underscore is escaped: a bare 'pg_%' is 'pg' plus any
    -- character, which would spare schemas like pgdog_schema_test.
    FOR s IN SELECT nspname FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public', 'pgdog')
        AND nspname NOT LIKE 'pg\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP SCHEMA %I CASCADE', s);
    END LOOP;
END $$;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO pgdog;
CREATE TABLE public.orgs (id VARCHAR PRIMARY KEY, shard_id BIGINT NOT NULL);
CREATE TABLE public.data (id BIGSERIAL, org_id VARCHAR NOT NULL, value TEXT, PRIMARY KEY (id, org_id));
ALTER SEQUENCE public.data_id_seq RESTART WITH 1 INCREMENT BY 2;

\c pgdog2
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%';
DO $$
DECLARE s TEXT;
BEGIN
    FOR s IN SELECT nspname FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public', 'pgdog')
        AND nspname NOT LIKE 'pg\_%' ESCAPE '\'
    LOOP
        EXECUTE format('DROP SCHEMA %I CASCADE', s);
    END LOOP;
END $$;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO pgdog;
CREATE TABLE public.orgs (id VARCHAR PRIMARY KEY, shard_id BIGINT NOT NULL);
CREATE TABLE public.data (id BIGSERIAL, org_id VARCHAR NOT NULL, value TEXT, PRIMARY KEY (id, org_id));
ALTER SEQUENCE public.data_id_seq RESTART WITH 2 INCREMENT BY 2;
