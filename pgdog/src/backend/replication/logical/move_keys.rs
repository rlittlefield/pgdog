//! Scope of a single-key reshard (`MOVE KEYS`): which sharding key
//! values move, from which shard to which, and across which tables.
//!
//! Keys are held in their canonical text form (integers re-formatted,
//! UUIDs lowercased), the same form the WAL filter derives from
//! replicated tuples, so membership checks can't miss on formatting.

use std::collections::HashSet;

use pgdog_config::{DataType, LookupResult};

use crate::backend::Cluster;
use crate::backend::pool::{Guard, Request};
use crate::frontend::router::sharding::Value as ShardingValue;
use crate::net::messages::Format;
use crate::util::quote_literal;

use super::error::Error;

/// One sharded table whose rows move.
#[derive(Debug, Clone)]
pub struct MoveTable {
    pub schema: String,
    pub name: String,
    pub sharding_column: String,
    pub data_type: DataType,
}

/// Everything a single MOVE KEYS run covers, shared between the copy,
/// the WAL filter and the cutover.
#[derive(Debug)]
pub struct KeyMoveScope {
    keys: HashSet<String>,
    source: usize,
    target: usize,
    tables: Vec<MoveTable>,
    data_type: DataType,
}

impl KeyMoveScope {
    /// Build a scope from operator-provided keys. Keys canonicalize
    /// through the tables' shared data type; a key that doesn't parse
    /// as that type is refused, and so is a table set that disagrees
    /// on the type.
    pub fn new(
        keys: &[String],
        source: usize,
        target: usize,
        tables: Vec<MoveTable>,
    ) -> Result<Self, Error> {
        let data_type = tables
            .first()
            .map(|table| table.data_type)
            .ok_or(Error::KeyMoveNoTables)?;
        if let Some(table) = tables.iter().find(|table| table.data_type != data_type) {
            return Err(Error::KeyMoveDataTypeMismatch {
                table: format!("\"{}\".\"{}\"", table.schema, table.name),
                expected: data_type,
                actual: table.data_type,
            });
        }

        let keys = keys
            .iter()
            .map(|key| canonical_key(key, data_type))
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(Self {
            keys,
            source,
            target,
            tables,
            data_type,
        })
    }

    /// Is this canonical value one of the moving keys? The value must
    /// come from [`Self::canonical`] or an equivalent canonical form.
    pub fn contains(&self, canonical: &str) -> bool {
        self.keys.contains(canonical)
    }

    /// Canonicalize a key value the same way the scope's keys were.
    pub fn canonical(&self, value: &str) -> Result<String, Error> {
        canonical_key(value, self.data_type)
    }

    /// The shared data type of the sharding column.
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// The shard the keys currently live on.
    pub fn source(&self) -> usize {
        self.source
    }

    /// The shard the keys move to.
    pub fn target(&self) -> usize {
        self.target
    }

    /// The tables whose rows move.
    pub fn tables(&self) -> &[MoveTable] {
        &self.tables
    }

    /// The moving keys, canonical.
    pub fn keys(&self) -> &HashSet<String> {
        &self.keys
    }

    /// A WHERE predicate matching the moving keys' rows, with the
    /// values inlined as quoted literals: used where binding isn't
    /// possible, e.g. `COPY (SELECT ...)`. Quoted literals are
    /// unknown-typed, so Postgres coerces them to the column's type.
    pub fn predicate_sql(&self, column: &str) -> String {
        let mut keys = self.keys.iter().collect::<Vec<_>>();
        keys.sort();
        format!(
            r#""{}" = ANY(ARRAY[{}])"#,
            column,
            keys.iter()
                .map(|key| quote_literal(key))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// The canonical text form of a key: integers re-formatted through
/// `i64`, UUIDs through the uuid parser (lowercased), text as is.
fn canonical_key(value: &str, data_type: DataType) -> Result<String, Error> {
    let sharding_value = ShardingValue::new(value, data_type);
    match sharding_value.to_text() {
        Ok(Some(text)) => Ok(text.into_owned()),
        _ => Err(Error::KeyMoveBadKey {
            data_type,
            value: value.to_string(),
        }),
    }
}

/// Every sharded table must place rows via `lookup_result = "shard"`:
/// that's the only placement a single key can flip. Stricter than
/// `placement_stable` (ADD SHARD), which also allows static mappings.
/// Each must also carry the `move_query` that performs the flip.
pub fn placement_by_lookup(cluster: &Cluster) -> Result<(), Error> {
    for table in cluster.sharded_tables() {
        let name = || {
            format!(
                "table \"{}\", column \"{}\"",
                table.name.as_deref().unwrap_or("*"),
                table.column,
            )
        };
        if table.lookup_result != LookupResult::Shard {
            return Err(Error::PlacementNotByLookup(name()));
        }
        if table.move_query.is_none() {
            return Err(Error::KeyMoveNoMoveQuery(name()));
        }
    }
    Ok(())
}

/// Advisory lock key taken on shard 0 of the database (ASCII
/// "pgdog_mv"): arbitrates which pgdog instance runs `MOVE KEYS` when
/// several share the same config.
pub(crate) const MOVE_KEYS_LOCK: i64 = 0x7067646f675f6d76;

/// Take the cross-instance key-move lock on shard 0 of `cluster`.
/// Session-scoped: the caller must keep the returned connection
/// checked out for as long as the task runs; a crashed holder releases
/// it with its connection.
pub(crate) async fn move_lock(cluster: &Cluster) -> Result<Guard, Error> {
    let mut server = cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?;

    let locked: Vec<String> = server
        .fetch_all(format!("SELECT pg_try_advisory_lock({})", MOVE_KEYS_LOCK).as_str())
        .await?;

    if locked.first().map(|l| l == "t").unwrap_or(false) {
        Ok(server)
    } else {
        Err(Error::KeyMoveLocked)
    }
}

/// Tables bearing a sharding column, resolved on the source shard's
/// primary: named `[[sharded_tables]]` entries directly, column-only
/// entries by asking the catalog which tables carry the column. Tables
/// declared omnisharded are excluded: their writes broadcast and their
/// rows don't move.
pub(crate) async fn enumerate_tables(
    cluster: &Cluster,
    source_shard: usize,
    omnisharded: &[String],
) -> Result<Vec<MoveTable>, Error> {
    let mut tables: Vec<MoveTable> = vec![];
    let mut seen = HashSet::new();

    let mut server = cluster
        .shards()
        .get(source_shard)
        .ok_or(crate::backend::pool::Error::NoShard(source_shard))?
        .primary(&Request::default())
        .await?;

    for rule in cluster.sharded_tables() {
        if let Some(name) = &rule.name {
            let schema = rule.schema.clone().unwrap_or_else(|| "public".into());
            if seen.insert((schema.clone(), name.clone())) {
                tables.push(MoveTable {
                    schema,
                    name: name.clone(),
                    sharding_column: rule.column.clone(),
                    data_type: rule.data_type,
                });
            }
            continue;
        }

        // Column-only rule: every regular table bearing the column,
        // matching how the router shards them.
        let params = [crate::net::bind::Parameter::new(rule.column.as_bytes())];
        let rows: Vec<crate::net::messages::DataRow> = server
            .fetch_all_params(
                "SELECT n.nspname, c.relname
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE a.attname = $1 AND NOT a.attisdropped AND a.attnum > 0
                   AND c.relkind = 'r'
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pgdog')
                 ORDER BY 1, 2",
                &params,
            )
            .await?;
        for row in rows {
            let schema: String = row.get(0, Format::Text).unwrap_or_default();
            let name: String = row.get(1, Format::Text).unwrap_or_default();
            if omnisharded.contains(&name) {
                continue;
            }
            if seen.insert((schema.clone(), name.clone())) {
                tables.push(MoveTable {
                    schema,
                    name,
                    sharding_column: rule.column.clone(),
                    data_type: rule.data_type,
                });
            }
        }
    }

    Ok(tables)
}

/// Every moving table's replica identity must cover its sharding
/// column: DELETE and identity-only UPDATE events carry only identity
/// columns, and the WAL filter can't judge a change whose key it can't
/// see. Checked on the source shard's primary; refused with a hint.
pub(crate) async fn replica_identity_covers_key(
    cluster: &Cluster,
    source_shard: usize,
    tables: &[MoveTable],
) -> Result<(), Error> {
    let mut server = cluster
        .shards()
        .get(source_shard)
        .ok_or(crate::backend::pool::Error::NoShard(source_shard))?
        .primary(&Request::default())
        .await?;

    for table in tables {
        let params = [
            crate::net::bind::Parameter::new(table.schema.as_bytes()),
            crate::net::bind::Parameter::new(table.name.as_bytes()),
            crate::net::bind::Parameter::new(table.sharding_column.as_bytes()),
        ];
        let rows: Vec<crate::net::messages::DataRow> = server
            .fetch_all_params(
                "SELECT CASE c.relreplident
                    WHEN 'f' THEN true
                    WHEN 'd' THEN EXISTS (
                        SELECT 1 FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = c.oid AND i.indisprimary AND a.attname = $3
                    )
                    WHEN 'i' THEN EXISTS (
                        SELECT 1 FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = c.oid AND i.indisreplident AND a.attname = $3
                    )
                    ELSE false
                 END
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2",
                &params,
            )
            .await?;

        let covered = rows
            .first()
            .and_then(|row| row.get::<String>(0, Format::Text))
            .map(|value| value == "t" || value == "true")
            .unwrap_or(false);
        if !covered {
            return Err(Error::KeyMoveIdentityGap {
                table: format!("\"{}\".\"{}\"", table.schema, table.name),
                column: table.sharding_column.clone(),
            });
        }
    }

    Ok(())
}

/// The target shard must hold no rows for the moving keys: leftovers
/// from a crashed prior attempt would collide with the copy. Refused
/// with the cleanup DELETE in the error.
pub(crate) async fn target_is_clean(cluster: &Cluster, scope: &KeyMoveScope) -> Result<(), Error> {
    let mut server = cluster
        .shards()
        .get(scope.target())
        .ok_or(crate::backend::pool::Error::NoShard(scope.target()))?
        .primary(&Request::default())
        .await?;

    for table in scope.tables() {
        let predicate = scope.predicate_sql(&table.sharding_column);
        let sql = format!(
            "SELECT EXISTS (SELECT 1 FROM \"{}\".\"{}\" WHERE {})",
            table.schema, table.name, predicate
        );
        let exists: Vec<String> = server.fetch_all(sql.as_str()).await?;
        if exists.first().map(|e| e == "t").unwrap_or(false) {
            return Err(Error::KeyMoveTargetDirty {
                table: format!("\"{}\".\"{}\"", table.schema, table.name),
                cleanup: format!(
                    "DELETE FROM \"{}\".\"{}\" WHERE {}",
                    table.schema, table.name, predicate
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::sharding::ShardedTable;

    fn tables(data_type: DataType) -> Vec<MoveTable> {
        vec![MoveTable {
            schema: "public".into(),
            name: "orders".into(),
            sharding_column: "tenant_id".into(),
            data_type,
        }]
    }

    #[test]
    fn test_keys_canonicalize() {
        let scope = KeyMoveScope::new(&["011".into(), "12".into()], 0, 2, tables(DataType::Bigint))
            .unwrap();
        // Integers canonicalize through i64.
        assert!(scope.contains("11"));
        assert!(!scope.contains("011"));
        assert!(scope.contains(&scope.canonical("012").unwrap()));
        assert!(!scope.contains("13"));

        // UUIDs canonicalize to lowercase.
        let scope = KeyMoveScope::new(
            &["550E8400-E29B-41D4-A716-446655440000".into()],
            0,
            1,
            tables(DataType::Uuid),
        )
        .unwrap();
        assert!(scope.contains("550e8400-e29b-41d4-a716-446655440000"));

        // A key that doesn't parse as the column type is refused.
        assert!(
            KeyMoveScope::new(&["not_a_number".into()], 0, 1, tables(DataType::Bigint)).is_err()
        );
    }

    #[test]
    fn test_predicate_sql() {
        let scope = KeyMoveScope::new(
            &["O'Brien".into(), "Acme".into()],
            0,
            1,
            tables(DataType::Varchar),
        )
        .unwrap();
        assert_eq!(
            scope.predicate_sql("tenant_id"),
            r#""tenant_id" = ANY(ARRAY['Acme', 'O''Brien'])"#
        );
    }

    #[test]
    fn test_data_types_must_agree() {
        let mut mixed = tables(DataType::Bigint);
        mixed.push(MoveTable {
            schema: "public".into(),
            name: "users".into(),
            sharding_column: "tenant_id".into(),
            data_type: DataType::Varchar,
        });
        assert!(KeyMoveScope::new(&["11".into()], 0, 1, mixed).is_err());
        assert!(KeyMoveScope::new(&["11".into()], 0, 1, vec![]).is_err());
    }

    #[test]
    fn test_placement_by_lookup() {
        use crate::backend::{Cluster, ShardedTables};
        use pgdog_config::{ConfigAndUsers, SystemCatalogsBehavior};

        let cluster = |lookup_result, move_query: Option<&str>| {
            let table = ShardedTable {
                database: "pgdog".into(),
                column: "tenant_id".into(),
                lookup_query: Some("SELECT shard_id FROM tenants WHERE id = $1".into()),
                lookup_result,
                move_query: move_query.map(|q| q.to_string()),
                ..Default::default()
            };
            let mut cluster = Cluster::new_test(&ConfigAndUsers::default());
            cluster.set_sharded_tables(ShardedTables::new(
                vec![table],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ));
            cluster
        };

        let move_query = "UPDATE tenants SET shard_id = $2 WHERE id = $1";
        placement_by_lookup(&cluster(LookupResult::Shard, Some(move_query))).unwrap();
        // Hashed placement can't flip a single key.
        assert!(placement_by_lookup(&cluster(LookupResult::Value, Some(move_query))).is_err());
        // Without a move_query there's nothing to flip it with.
        assert!(placement_by_lookup(&cluster(LookupResult::Shard, None)).is_err());
    }
}
