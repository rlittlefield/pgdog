#!/bin/bash
# Integration test: MOVE KEYS — move sharding keys' rows from one shard
# to another semi-live, flip their placement, and clean up.
#
# Requires:
#   - local postgres at port 5432 with databases pgdog1, pgdog2
#     (created by integration/setup.sh)
#   - wal_level = logical, max_replication_slots >= 8
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEFAULT_BIN="${SCRIPT_DIR}/../../target/debug/pgdog"
PGDOG_BIN=${PGDOG_BIN:-$DEFAULT_BIN}
PGDOG_CONFIG="${SCRIPT_DIR}/pgdog.toml"
PGDOG_USERS="${SCRIPT_DIR}/users.toml"

export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=pgdog
export PGPASSWORD=pgdog

PGDOG_PORT=6499
PGDOG2_PORT=6500
PGDOG_PID=""
PGDOG2_PID=""
WRITER_PID=""

app() {
    psql "host=127.0.0.1 port=${PGDOG_PORT} dbname=pgdog user=pgdog password=pgdog" -tAc "$1"
}

app2() {
    psql "host=127.0.0.1 port=${PGDOG2_PORT} dbname=pgdog user=pgdog password=pgdog" -tAc "$1"
}

admin() {
    psql "host=127.0.0.1 port=${PGDOG_PORT} dbname=admin user=admin password=pgdog" -tAc "$1"
}

direct() {
    local db=$1 sql=$2
    psql -d "$db" -tAc "$sql"
}

cleanup() {
    if [ -n "${WRITER_PID}" ]; then
        kill ${WRITER_PID} 2>/dev/null || true
        wait ${WRITER_PID} 2>/dev/null || true
    fi
    if [ -n "${PGDOG2_PID}" ]; then
        kill ${PGDOG2_PID} 2>/dev/null || true
        wait ${PGDOG2_PID} 2>/dev/null || true
    fi
    if [ -n "${PGDOG_PID}" ]; then
        kill ${PGDOG_PID} 2>/dev/null || true
        wait ${PGDOG_PID} 2>/dev/null || true
    fi
    rm -f "${SCRIPT_DIR}/pgdog2.toml" "${SCRIPT_DIR}/pgdog_abort.toml"
}
trap cleanup EXIT

start_pgdog() {
    local cfg=${1:-${PGDOG_CONFIG}}
    ${PGDOG_BIN} --config "${cfg}" --users "${PGDOG_USERS}" &
    PGDOG_PID=$!
    for _ in $(seq 1 50); do
        if app "SELECT 1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "ERROR: pgdog did not start"
    exit 1
}

stop_pgdog() {
    if [ -n "${PGDOG_PID}" ]; then
        kill ${PGDOG_PID} 2>/dev/null || true
        wait ${PGDOG_PID} 2>/dev/null || true
        PGDOG_PID=""
    fi
}

# A second pgdog instance sharing the config, on its own port.
start_pgdog2() {
    sed "s/port = ${PGDOG_PORT}/port = ${PGDOG2_PORT}/" "${PGDOG_CONFIG}" > "${SCRIPT_DIR}/pgdog2.toml"
    ${PGDOG_BIN} --config "${SCRIPT_DIR}/pgdog2.toml" --users "${PGDOG_USERS}" &
    PGDOG2_PID=$!
    for _ in $(seq 1 50); do
        if app2 "SELECT 1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.2
    done
    echo "ERROR: second pgdog did not start"
    exit 1
}

stop_pgdog2() {
    if [ -n "${PGDOG2_PID}" ]; then
        kill ${PGDOG2_PID} 2>/dev/null || true
        wait ${PGDOG2_PID} 2>/dev/null || true
        PGDOG2_PID=""
    fi
}

# A writer inserting data rows for one org through pgdog. Counts its
# successful writes in a file so tests can assert none were lost.
start_writer() {
    local org=$1 counter=$2
    : > "${counter}"
    (
        i=0
        while true; do
            i=$((i + 1))
            if app "INSERT INTO data (org_id, value) VALUES ('${org}', 'w${i}')" >/dev/null 2>&1; then
                echo "${i}" >> "${counter}"
            fi
            sleep 0.1
        done
    ) &
    WRITER_PID=$!
}

stop_writer() {
    if [ -n "${WRITER_PID}" ]; then
        kill ${WRITER_PID} 2>/dev/null || true
        wait ${WRITER_PID} 2>/dev/null || true
        WRITER_PID=""
    fi
}

# wait_for_status TASK_ID STATUS TIMEOUT_SECS
wait_for_status() {
    local task_id=$1 status=$2 timeout=$3
    for _ in $(seq 1 $((timeout * 2))); do
        local current
        current=$(admin "SHOW TASKS" | grep "^${task_id}|" | cut -d'|' -f5 || true)
        if [ "${current}" = "${status}" ]; then
            return 0
        fi
        local lifecycle
        lifecycle=$(admin "SHOW TASKS" | grep "^${task_id}|" | cut -d'|' -f4 || true)
        if [[ "${lifecycle}" == error* || "${lifecycle}" == failed* ]]; then
            echo "ERROR: task ${task_id} failed: $(admin "SHOW TASKS" | grep "^${task_id}|")"
            return 1
        fi
        sleep 0.5
    done
    echo "ERROR: task ${task_id} never reached status '${status}'"
    admin "SHOW TASKS" || true
    return 1
}

# wait_for_finished TASK_ID TIMEOUT_SECS
wait_for_finished() {
    local task_id=$1 timeout=$2
    local lifecycle=""
    for _ in $(seq 1 $((timeout * 2))); do
        lifecycle=$(admin "SHOW TASKS" | grep "^${task_id}|" | cut -d'|' -f4 || true)
        [ "${lifecycle}" = "finished" ] && return 0
        if [[ "${lifecycle}" == error* || "${lifecycle}" == failed* ]]; then
            echo "ERROR: task ${task_id} failed: $(admin "SHOW TASKS" | grep "^${task_id}|")"
            return 1
        fi
        sleep 0.5
    done
    echo "ERROR: task ${task_id} never finished"
    admin "SHOW TASKS" || true
    return 1
}

reset_all() {
    stop_writer
    stop_pgdog2
    stop_pgdog
    psql -d pgdog1 -f "${SCRIPT_DIR}/init.sql" >/dev/null
    start_pgdog

    # Seed: two orgs on shard 0, one on shard 1, plus their data.
    app "INSERT INTO orgs (id, shard_id) VALUES ('org_a', 0), ('org_b', 0), ('org_c', 1)" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_a', 'a1')" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_a', 'a2')" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_b', 'b1')" >/dev/null
    app "INSERT INTO data (org_id, value) VALUES ('org_c', 'c1')" >/dev/null
}

assert_eq() {
    local left=$1 right=$2 message=$3
    if [ "${left}" != "${right}" ]; then
        echo "ASSERTION FAILED: ${message} (${left} != ${right})"
        exit 1
    fi
}

pushd "${SCRIPT_DIR}" >/dev/null

echo "=== Test A: basic two-key move ==="
reset_all

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a,org_b AUTO")
echo "task: ${TASK_ID}"
wait_for_finished "${TASK_ID}" 120

# The rows moved: on shard 1, gone from shard 0.
ON_TARGET=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id IN ('org_a', 'org_b')")
assert_eq "${ON_TARGET}" "3" "moved rows landed on the target shard"
ON_SOURCE=$(direct pgdog1 "SELECT count(*) FROM data WHERE org_id IN ('org_a', 'org_b')")
assert_eq "${ON_SOURCE}" "0" "moved rows deleted from the source shard"

# The placement flipped on every shard's copy of the mapping.
for db in pgdog1 pgdog2; do
    FLIPPED=$(direct "$db" "SELECT count(*) FROM orgs WHERE id IN ('org_a', 'org_b') AND shard_id = 1")
    assert_eq "${FLIPPED}" "2" "placement flipped on ${db}"
done

# The unmoved key is untouched.
C_ROWS=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_c'")
assert_eq "${C_ROWS}" "1" "non-moving key untouched"

# Traffic through pgdog reads and writes the target shard.
READ=$(app "SELECT count(*) FROM data WHERE org_id = 'org_a'")
assert_eq "${READ}" "2" "moved key readable through pgdog"
app "INSERT INTO data (org_id, value) VALUES ('org_a', 'post')" >/dev/null
POST=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_a' AND value = 'post'")
assert_eq "${POST}" "1" "post-move write landed on the target shard"

# No replication slots left behind.
SLOTS=$(direct pgdog1 "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%'")
assert_eq "${SLOTS}" "0" "replication slots cleaned up"

echo "=== Test B: live writer on a moving key loses nothing ==="
reset_all
COUNTER="${SCRIPT_DIR}/.writer_count"
start_writer org_a "${COUNTER}"
sleep 1

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a")
wait_for_status "${TASK_ID}" "awaiting cutover" 120
# The writer keeps making progress while the task is parked.
BEFORE=$(wc -l < "${COUNTER}")
sleep 2
AFTER=$(wc -l < "${COUNTER}")
if [ "${AFTER}" -le "${BEFORE}" ]; then
    echo "ASSERTION FAILED: writer stalled while task parked"
    exit 1
fi

admin "CUTOVER ${TASK_ID}" >/dev/null
wait_for_finished "${TASK_ID}" 120
sleep 1
stop_writer

# Every acknowledged write exists exactly once, all on the target.
WRITTEN=$(wc -l < "${COUNTER}" | tr -d ' ')
ON_TARGET=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_a' AND value LIKE 'w%'")
ON_SOURCE=$(direct pgdog1 "SELECT count(*) FROM data WHERE org_id = 'org_a'")
assert_eq "${ON_SOURCE}" "0" "no rows left on the source shard"
if [ "${ON_TARGET}" -lt "${WRITTEN}" ]; then
    echo "ASSERTION FAILED: writes lost during the move (${ON_TARGET} < ${WRITTEN})"
    exit 1
fi
rm -f "${COUNTER}"

echo "=== Test C: abort while parked leaves placement unchanged ==="
reset_all
TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a")
wait_for_status "${TASK_ID}" "awaiting cutover" 120
admin "STOP_TASK ${TASK_ID}" >/dev/null
for _ in $(seq 1 120); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    case "${LIFECYCLE}" in cancelled|finished) break ;; esac
    sleep 0.5
done

# Placement unchanged; the copied rows were scrubbed from the target.
PLACEMENT=$(direct pgdog1 "SELECT shard_id FROM orgs WHERE id = 'org_a'")
assert_eq "${PLACEMENT}" "0" "placement unchanged after abort"
sleep 2
SCRUBBED=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_a'")
assert_eq "${SCRUBBED}" "0" "copied rows scrubbed from the target"
SLOTS=$(direct pgdog1 "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '__pgdog%'")
assert_eq "${SLOTS}" "0" "replication slots cleaned up after abort"

# The key is still writable and still routes to the source shard.
app "INSERT INTO data (org_id, value) VALUES ('org_a', 'after_abort')" >/dev/null
STILL_SOURCE=$(direct pgdog1 "SELECT count(*) FROM data WHERE org_id = 'org_a' AND value = 'after_abort'")
assert_eq "${STILL_SOURCE}" "1" "key still routes to the source after abort"

# A retry succeeds from the clean state.
TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a AUTO")
wait_for_finished "${TASK_ID}" 120

echo "=== Test D: replica identity not covering the key is refused ==="
reset_all
# A table whose PK (and so replica identity) misses org_id: DELETEs
# replicated from it wouldn't carry the key.
direct pgdog1 "CREATE TABLE public.ri_gap (id BIGINT PRIMARY KEY, org_id VARCHAR NOT NULL)" >/dev/null
direct pgdog2 "CREATE TABLE public.ri_gap (id BIGINT PRIMARY KEY, org_id VARCHAR NOT NULL)" >/dev/null

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a")
LIFECYCLE=""
for _ in $(seq 1 60); do
    LIFECYCLE=$(admin "SHOW TASKS" | grep "^${TASK_ID}|" | cut -d'|' -f4 || true)
    [[ "${LIFECYCLE}" == failed* ]] && break
    sleep 0.5
done
if [[ "${LIFECYCLE}" != failed*"replica identity"* ]]; then
    echo "ASSERTION FAILED: move should refuse a replica-identity gap (${LIFECYCLE})"
    exit 1
fi

# With the identity fixed, the same move goes through.
direct pgdog1 "ALTER TABLE public.ri_gap REPLICA IDENTITY FULL" >/dev/null
direct pgdog2 "ALTER TABLE public.ri_gap REPLICA IDENTITY FULL" >/dev/null
TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a AUTO")
wait_for_finished "${TASK_ID}" 120
direct pgdog1 "DROP TABLE public.ri_gap" >/dev/null
direct pgdog2 "DROP TABLE public.ri_gap" >/dev/null

echo "=== Test E: foreign keys between moving tables delete in order ==="
reset_all
for db in pgdog1 pgdog2; do
    direct "$db" "CREATE TABLE public.parent (id BIGINT, org_id VARCHAR NOT NULL, PRIMARY KEY (id, org_id))" >/dev/null
    direct "$db" "CREATE TABLE public.child (id BIGINT, org_id VARCHAR NOT NULL, parent_id BIGINT, \
        PRIMARY KEY (id, org_id), FOREIGN KEY (parent_id, org_id) REFERENCES public.parent (id, org_id))" >/dev/null
done
direct pgdog1 "INSERT INTO public.parent (id, org_id) VALUES (1, 'org_a')" >/dev/null
direct pgdog1 "INSERT INTO public.child (id, org_id, parent_id) VALUES (1, 'org_a', 1)" >/dev/null

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a AUTO")
wait_for_finished "${TASK_ID}" 120

PARENT_MOVED=$(direct pgdog2 "SELECT count(*) FROM parent WHERE org_id = 'org_a'")
CHILD_MOVED=$(direct pgdog2 "SELECT count(*) FROM child WHERE org_id = 'org_a'")
assert_eq "${PARENT_MOVED}" "1" "parent row moved"
assert_eq "${CHILD_MOVED}" "1" "child row moved"
LEFT=$(direct pgdog1 "SELECT (SELECT count(*) FROM parent) + (SELECT count(*) FROM child)")
assert_eq "${LEFT}" "0" "source rows deleted children-first"

echo "=== Test F: non-moving keys never block during the cutover ==="
reset_all
COUNTER="${SCRIPT_DIR}/.writer_count_c"
start_writer org_c "${COUNTER}"
sleep 1

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a AUTO")
wait_for_finished "${TASK_ID}" 120
sleep 1
stop_writer

# The org_c writer never lost a write: everything it acked exists.
WRITTEN=$(wc -l < "${COUNTER}" | tr -d ' ')
STORED=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_c' AND value LIKE 'w%'")
if [ "${STORED}" -lt "${WRITTEN}" ]; then
    echo "ASSERTION FAILED: non-moving key lost writes during the cutover (${STORED} < ${WRITTEN})"
    exit 1
fi
rm -f "${COUNTER}"

echo "=== Test G: an instance started after the move routes fresh ==="
reset_all
TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a AUTO")
wait_for_finished "${TASK_ID}" 120

# A fresh instance has an empty cache and reads the flipped placement.
start_pgdog2
app2 "INSERT INTO data (org_id, value) VALUES ('org_a', 'fresh')" >/dev/null
ON_TARGET=$(direct pgdog2 "SELECT count(*) FROM data WHERE org_id = 'org_a' AND value = 'fresh'")
assert_eq "${ON_TARGET}" "1" "fresh instance routed to the target"
stop_pgdog2

echo "=== Test H: a drain that can't converge re-parks and releases ==="
reset_all
# A short timeout with the abort action: a busy writer on a NON-moving
# key of the source shard streams commits the filter skips, keeping the
# last transaction fresher than the drain requires
# (cutover_last_transaction_delay, 1s), so the cutover aborts and
# re-parks. (A writer on the moving key can't do this: the barrier
# parks it the moment the cutover arms.) Without the writer the drain
# converges within the window.
stop_pgdog
sed "s/^\[general\]$/[general]\ncutover_timeout = 3000\ncutover_timeout_action = \"abort\"/" \
    "${PGDOG_CONFIG}" > "${SCRIPT_DIR}/pgdog_abort.toml"
start_pgdog "${SCRIPT_DIR}/pgdog_abort.toml"

app "INSERT INTO orgs (id, shard_id) VALUES ('org_d', 0) ON CONFLICT DO NOTHING" >/dev/null

# One long-lived session writing every 200ms: per-write psql processes
# are too slow to keep the last transaction consistently fresh.
{ echo "INSERT INTO data (org_id, value) VALUES ('org_d', 'wi_probe');"; echo '\watch 0.2'; } | \
    psql "host=127.0.0.1 port=${PGDOG_PORT} dbname=pgdog user=pgdog password=pgdog" >/dev/null 2>&1 &
WRITER_PID=$!
sleep 1

TASK_ID=$(admin "MOVE KEYS pgdog 1 org_a")
wait_for_status "${TASK_ID}" "awaiting cutover" 120
admin "CUTOVER ${TASK_ID}" >/dev/null
# The drain aborts and the task re-parks.
sleep 4
wait_for_status "${TASK_ID}" "awaiting cutover" 60

# The barrier was released: a write for the moving key succeeds.
app "INSERT INTO data (org_id, value) VALUES ('org_a', 'after_repark')" >/dev/null

# Without the writer the drain converges and the cutover completes.
kill ${WRITER_PID} 2>/dev/null || true
wait ${WRITER_PID} 2>/dev/null || true
WRITER_PID=""
# The psql inside the container may outlive the shim: terminate its
# backend so the writes actually stop.
direct pgdog1 "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
    WHERE query LIKE '%wi_probe%' AND pid != pg_backend_pid()" >/dev/null || true
sleep 2
admin "CUTOVER ${TASK_ID}" >/dev/null
wait_for_finished "${TASK_ID}" 120
ON_SOURCE=$(direct pgdog1 "SELECT count(*) FROM data WHERE org_id = 'org_a'")
assert_eq "${ON_SOURCE}" "0" "rows moved after the retried cutover"

popd >/dev/null
echo "move_keys integration tests passed"
