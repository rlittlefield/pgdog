//! Sharded tables whose NULL-key rows replicate to every shard.

/// A sharded table configured with `broadcast_null`: rows whose
/// sharding key is NULL exist on every shard, and ADD SHARD copies and
/// replicates exactly those rows to the new shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridNullTable {
    /// Schema the config names; `None` matches any schema.
    pub schema: Option<String>,
    pub name: String,
    /// The sharding key column: only rows where it's NULL replicate.
    pub column: String,
}

impl HybridNullTable {
    /// Does this config entry cover the given table?
    pub fn matches(&self, schema: &str, name: &str) -> bool {
        self.name == name && self.schema.as_deref().is_none_or(|s| s == schema)
    }
}
