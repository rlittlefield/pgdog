//! Configuration.

// Submodules
pub(crate) mod changed;
pub(crate) mod convert;
pub(crate) mod core;
pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod general;
pub(crate) mod memory;
pub(crate) mod networking;
pub(crate) mod overrides;
pub(crate) mod pooling;
pub(crate) mod replication;
pub(crate) mod rewrite;
pub(crate) mod sharding;
pub(crate) mod users;

pub(crate) use core::{Config, ConfigAndUsers};
pub(crate) use database::{Database, Role};
pub(crate) use error::Error;
pub(crate) use general::General;
pub(crate) use memory::*;
pub(crate) use networking::{MultiTenant, TlsVerifyMode};
pub(crate) use overrides::Overrides;
use pgdog_config::LookupResult;
pub(crate) use pgdog_config::auth::AuthType;
pub(crate) use pgdog_config::{
    LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy, ServerTls,
};
pub(crate) use pooling::{ConnectionRecovery, PoolerMode, PreparedStatementsLevel};
pub(crate) use rewrite::RewriteMode;
use std::path::Path;
#[cfg(test)]
pub(crate) use users::Users;
pub(crate) use users::{ServerAuth, User};

// Re-export from sharding module
pub(crate) use sharding::{DataType, Hasher, ShardedMappingDeprecated};

// Re-export from replication module
pub(crate) use replication::MirrorConfig;

use parking_lot::Mutex;
use std::env;
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

static CONFIG: Lazy<ArcSwap<ConfigAndUsers>> =
    Lazy::new(|| ArcSwap::from_pointee(ConfigAndUsers::default()));

static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Load configuration.
pub(crate) fn config() -> Arc<ConfigAndUsers> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub(crate) fn load(config: &Path, users: &Path) -> Result<ConfigAndUsers, Error> {
    let config = ConfigAndUsers::load(config, users)?;
    set(config)
}

pub(crate) fn set(mut config: ConfigAndUsers) -> Result<ConfigAndUsers, Error> {
    config.check()?;
    validate_lookup_queries(&config)?;
    for table in config.config.sharded_tables.iter_mut() {
        // TODO: synchronous io operations inside that could be parallelized.
        // And also moved outside the configuration to the place of
        table.load_centroids()?;
    }
    CONFIG.store(Arc::new(config.clone()));
    changed::notify(CONFIG.load_full());
    Ok(config)
}

/// Validate sharding key lookup and move queries with the SQL parser:
/// each must be syntactically valid, a single statement, and reference
/// exactly the expected parameters (`$1` for `lookup_query`; `$1` and
/// `$2` for `move_query`, which must also be an UPDATE).
/// `lookup_result = "shard"` requires a lookup query and never hashes,
/// so every hash-related setting on such a table is a contradiction and
/// fails loudly.
fn validate_lookup_queries(config: &ConfigAndUsers) -> Result<(), Error> {
    for table in &config.config.sharded_tables {
        let error = |message: String| {
            Error::ParseError(format!(
                "sharded table \"{}\", column \"{}\": {}",
                table.name.as_deref().unwrap_or("*"),
                table.column,
                message,
            ))
        };

        if table.lookup_result == LookupResult::Shard {
            if table.lookup_query.is_none() {
                return Err(error(
                    "\"lookup_result = 'shard'\" requires a \"lookup_query\"".into(),
                ));
            }
            if table.mapping.is_some() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with \"mapping\""
                        .into(),
                ));
            }
            if !table.centroids.is_empty() || table.centroids_path.is_some() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with centroids"
                        .into(),
                ));
            }
            if table.hasher != Hasher::default() {
                return Err(error(
                    "\"lookup_result = 'shard'\" never hashes and can't be combined with \"hasher\""
                        .into(),
                ));
            }
        }

        if let Some(query) = &table.move_query {
            if table.lookup_result != LookupResult::Shard {
                return Err(error(
                    "\"move_query\" requires a \"lookup_query\" with \"lookup_result = 'shard'\""
                        .into(),
                ));
            }
            validate_query(query, &MOVE_QUERY_RULES).map_err(error)?;
        }

        let Some(query) = &table.lookup_query else {
            continue;
        };

        validate_query(query, &LOOKUP_QUERY_RULES).map_err(error)?;
    }

    Ok(())
}

/// What a configured query must look like: which setting it came from,
/// the exact parameter set it must reference (`$1..=$params`), and
/// whether it has to be an UPDATE. A key being moved always has a
/// mapping row already (it resolved to its source shard), so the flip
/// never needs to insert one.
struct QueryRules {
    label: &'static str,
    params: i32,
    params_hint: &'static str,
    update: bool,
}

const LOOKUP_QUERY_RULES: QueryRules = QueryRules {
    label: "lookup_query",
    params: 1,
    params_hint: "exactly one parameter, \"$1\"",
    update: false,
};

const MOVE_QUERY_RULES: QueryRules = QueryRules {
    label: "move_query",
    params: 2,
    params_hint: "exactly two parameters, \"$1\" (the key) and \"$2\" (the shard)",
    update: true,
};

fn validate_query(query: &str, rules: &QueryRules) -> Result<(), String> {
    use itertools::Itertools;
    use pg_raw_parse::{
        Node,
        walk::{Recurse, walk_manual},
    };
    use std::collections::HashSet;
    use std::ops::ControlFlow;

    let ast = pg_raw_parse::parse(query)
        .map_err(|err| format!("\"{}\" is invalid: {}", rules.label, err))?;
    let stmt = ast
        .stmts()
        .exactly_one()
        .map_err(|_| format!("\"{}\" must be a single statement", rules.label))?;

    if rules.update && !matches!(stmt, Node::UpdateStmt(_)) {
        return Err(format!("\"{}\" must be an UPDATE", rules.label));
    }

    let mut seen = HashSet::new();
    let out_of_range = walk_manual(stmt, |node| match node {
        Node::ParamRef(param) if param.number < 1 || param.number > rules.params => {
            ControlFlow::Break(())
        }
        Node::ParamRef(param) => {
            seen.insert(param.number);
            Recurse::yes()
        }
        _ => Recurse::yes(),
    })
    .is_some();

    if out_of_range || seen.len() != rules.params as usize {
        return Err(format!(
            "\"{}\" must reference {}",
            rules.label, rules.params_hint
        ));
    }

    Ok(())
}

/// Load configuration from a list of database URLs.
pub(crate) fn from_urls(urls: &[String]) -> Result<ConfigAndUsers, Error> {
    let _lock = LOCK.lock();
    let config = (*config()).clone();
    let config = config.databases_from_urls(urls)?;
    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Extract all database URLs from the environment and
/// create the config.
pub(crate) fn from_env() -> Result<ConfigAndUsers, Error> {
    let _lock = LOCK.lock();

    let mut urls = vec![];
    let mut index = 1;
    while let Ok(url) = env::var(format!("PGDOG_DATABASE_URL_{}", index)) {
        urls.push(url);
        index += 1;
    }

    if urls.is_empty() {
        return Err(Error::NoDbsInEnv);
    }

    let mut config = (*config()).clone();
    config = config.databases_from_urls(&urls)?;

    // Extract mirroring configuration
    let mut mirror_strs = vec![];
    let mut index = 1;
    while let Ok(mirror_str) = env::var(format!("PGDOG_MIRRORING_{}", index)) {
        mirror_strs.push(mirror_str);
        index += 1;
    }

    if !mirror_strs.is_empty() {
        config = config.mirroring_from_strings(&mirror_strs)?;
    }

    CONFIG.store(Arc::new(config.clone()));
    Ok(config)
}

/// Override some settings.
pub(crate) fn overrides(overrides: Overrides) {
    let mut config = (*config()).clone();
    let Overrides {
        default_pool_size,
        min_pool_size,
        session_mode,
    } = overrides;

    if let Some(default_pool_size) = default_pool_size {
        config.config.general.default_pool_size = default_pool_size;
    }

    if let Some(min_pool_size) = min_pool_size {
        config.config.general.min_pool_size = min_pool_size;
    }

    if let Some(session_mode) = session_mode {
        config.config.general.pooler_mode = if session_mode {
            PoolerMode::Session
        } else {
            PoolerMode::Transaction
        };
    }

    CONFIG.store(Arc::new(config));
}

// Test helper functions
#[cfg(test)]
pub(crate) fn load_test() {
    load_test_with_pooler_mode(PoolerMode::Transaction)
}

#[cfg(test)]
pub(crate) fn load_test_with_pooler_mode(pooler_mode: PoolerMode) {
    load_test_with_user_and_pooler_mode("pgdog", pooler_mode, Role::default())
}

#[cfg(test)]
fn load_test_with_user_and_pooler_mode(user: &str, pooler_mode: PoolerMode, role: Role) {
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![Database {
        name: "pgdog".into(),
        host: "127.0.0.1".into(),
        port: 5432,
        pooler_mode: Some(pooler_mode),
        role,
        ..Default::default()
    }];
    config.users.users = vec![User {
        name: user.into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        pooler_mode: Some(pooler_mode),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

#[cfg(test)]
pub(crate) fn load_test_replicas() {
    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Primary,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Replica,
            read_only: Some(true),
            ..Default::default()
        },
    ];
    config.config.general.load_balancing_strategy = LoadBalancingStrategy::RoundRobin;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

#[cfg(test)]
pub(crate) fn load_test_sharded() {
    load_test_sharded_n(2);
}

/// Load 3-shard test configuration.
#[cfg(test)]
pub(crate) fn load_test_sharded_3() {
    load_test_sharded_n(3);
}

#[cfg(test)]
fn load_test_sharded_n(num_shards: usize) {
    use pgdog_config::{OmnishardedTables, ShardedSchema, ShardedTableConfig};

    use crate::backend::databases::init;

    let mut config = ConfigAndUsers::default();
    config.config.general.min_pool_size = 0;
    config.config.databases = (0..num_shards)
        .flat_map(|shard| {
            vec![
                Database {
                    name: "pgdog".into(),
                    host: "127.0.0.1".into(),
                    port: 5432,
                    role: Role::Primary,
                    database_name: Some(format!("shard_{}", shard)),
                    shard,
                    ..Default::default()
                },
                Database {
                    name: "pgdog".into(),
                    host: "127.0.0.1".into(),
                    port: 5432,
                    role: Role::Replica,
                    read_only: Some(true),
                    database_name: Some(format!("shard_{}", shard)),
                    shard,
                    ..Default::default()
                },
            ]
        })
        .collect();
    config.config.sharded_tables = vec![
        ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded".into()),
            column: "id".into(),
            ..Default::default()
        },
        ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded_varchar".into()),
            column: "id_varchar".into(),
            data_type: DataType::Varchar,
            ..Default::default()
        },
        ShardedTableConfig {
            database: "pgdog".into(),
            name: Some("sharded_uuid".into()),
            column: "id_uuid".into(),
            data_type: DataType::Uuid,
            ..Default::default()
        },
    ];
    config.config.sharded_schemas = vec![
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("acustomer".into()),
            shard: 0,
            ..Default::default()
        },
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("bcustomer".into()),
            shard: 1,
            ..Default::default()
        },
        ShardedSchema {
            database: "pgdog".into(),
            name: Some("all".into()),
            all: true,
            ..Default::default()
        },
    ];
    config.config.omnisharded_tables = vec![OmnishardedTables {
        database: "pgdog".into(),
        tables: vec!["sharded_omni".into()],
        sticky: false,
    }];
    config.config.rewrite.enabled = true;
    config.config.rewrite.split_inserts = RewriteMode::Rewrite;
    config.config.rewrite.shard_key = RewriteMode::Rewrite;
    config.config.general.load_balancing_strategy = LoadBalancingStrategy::RoundRobin;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

#[cfg(test)]
mod lookup_query_tests {
    use super::*;

    fn config_with_query(query: &str) -> ConfigAndUsers {
        let source = format!(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "{}"
"#,
            query
        );

        ConfigAndUsers {
            config: toml::from_str(&source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_lookup_query_valid() {
        let config =
            config_with_query("SELECT COALESCE(parent_tenant_id, id) FROM tenants WHERE id = $1");
        validate_lookup_queries(&config).unwrap();
    }

    fn config_from(source: &str) -> ConfigAndUsers {
        ConfigAndUsers {
            config: toml::from_str(source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_lookup_result_shard_requires_query() {
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_result = "shard"
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());
    }

    #[test]
    fn test_lookup_result_shard_never_hashes_config() {
        // Valid: query + shard mode, no hash-related settings.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
"#,
        );
        validate_lookup_queries(&config).unwrap();

        // Mapping contradicts shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"

[[sharded_tables.mapping]]
values = [1]
shard = 0
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());

        // Centroids contradict shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
centroids = [[1.0, 2.0]]
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());

        // A non-default hasher contradicts shard mode.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
hasher = "sha1"
"#,
        );
        assert!(validate_lookup_queries(&config).is_err());
    }

    #[test]
    fn test_lookup_query_requires_valid_sql() {
        let config = config_with_query("SELEC oops");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn test_lookup_query_requires_single_statement() {
        let config = config_with_query("SELECT 1; SELECT 2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("single statement"));
    }

    #[test]
    fn test_lookup_query_requires_placeholder() {
        let config = config_with_query("SELECT parent FROM tenants");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));

        let config = config_with_query("SELECT parent FROM tenants WHERE id = $1 AND x = $2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));

        let config = config_with_query("SELECT parent FROM tenants WHERE x = $2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$1"));
    }

    fn config_with_move_query(move_query: &str) -> ConfigAndUsers {
        let source = format!(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"
move_query = "{}"
"#,
            move_query
        );

        ConfigAndUsers {
            config: toml::from_str(&source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_move_query_valid() {
        let config = config_with_move_query("UPDATE tenants SET shard_id = $2 WHERE id = $1");
        validate_lookup_queries(&config).unwrap();
    }

    #[test]
    fn test_move_query_requires_shard_lookup() {
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
move_query = "UPDATE tenants SET shard_id = $2 WHERE id = $1"
"#,
        );
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("lookup_result"));

        // A lookup that hashes the translated value can't flip a key
        // to a chosen shard either.
        let config = config_from(
            r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT parent FROM tenants WHERE id = $1"
move_query = "UPDATE tenants SET shard_id = $2 WHERE id = $1"
"#,
        );
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("lookup_result"));
    }

    #[test]
    fn test_move_query_requires_update() {
        let config = config_with_move_query("SELECT shard_id FROM tenants WHERE id = $1 OR $2 = 0");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("must be an UPDATE"));

        let config = config_with_move_query("DELETE FROM tenants WHERE id = $1 AND shard_id = $2");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("must be an UPDATE"));
    }

    #[test]
    fn test_move_query_requires_both_params() {
        let config = config_with_move_query("UPDATE tenants SET shard_id = 0 WHERE id = $1");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$2"));

        let config = config_with_move_query("UPDATE tenants SET shard_id = $2 WHERE id = 1");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$2"));

        let config = config_with_move_query("UPDATE tenants SET shard_id = $2 WHERE id = $3");
        let err = validate_lookup_queries(&config).unwrap_err();
        assert!(err.to_string().contains("$2"));
    }
}
