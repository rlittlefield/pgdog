//! Scope of a single-key reshard (`MOVE KEYS`): which sharding key
//! values move, from which shard to which, and across which tables.
//!
//! Keys are held in their canonical text form (integers re-formatted,
//! UUIDs lowercased), the same form the WAL filter derives from
//! replicated tuples, so membership checks can't miss on formatting.

use std::collections::HashSet;

use pgdog_config::{DataType, LookupResult};

use crate::backend::Cluster;
use crate::frontend::router::sharding::Value as ShardingValue;
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
pub fn placement_by_lookup(cluster: &Cluster) -> Result<(), Error> {
    for table in cluster.sharded_tables() {
        if table.lookup_result != LookupResult::Shard {
            return Err(Error::PlacementNotByLookup(format!(
                "table \"{}\", column \"{}\"",
                table.name.as_deref().unwrap_or("*"),
                table.column,
            )));
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

        let cluster = |lookup_result| {
            let table = ShardedTable {
                database: "pgdog".into(),
                column: "tenant_id".into(),
                lookup_query: Some("SELECT shard_id FROM tenants WHERE id = $1".into()),
                lookup_result,
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

        placement_by_lookup(&cluster(LookupResult::Shard)).unwrap();
        assert!(placement_by_lookup(&cluster(LookupResult::Value)).is_err());
    }
}
