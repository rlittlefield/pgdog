mod directive;
mod strip;

#[cfg(test)]
mod tests;

use crate::backend::ShardingSchema;
use crate::config::database::Role;
use crate::frontend::router::sharding::ShardOrLookup;

use super::Error;
use strip::{leading_block_comment, trailing_block_comment};

#[derive(Default, Debug, Clone)]
pub struct QueryAndComment<'a> {
    pub query: &'a str,
    #[cfg(test)]
    pub comment: String,
    pub role: Option<Role>,
    pub shard: Option<ShardOrLookup>,
    /// The raw `pgdog_sharding_key` value the comment carried. Only
    /// recorded while a keyed write barrier is armed (MOVE KEYS), so
    /// the query engine can park writes for the moving keys.
    pub sharding_key: Option<String>,
}

/// Extract SQL C-style block comments from both the beginning and the end
/// of the query, returning the stripped query string and directives found
/// in either side. Leading takes precedence when both sides carry the same
/// directive (e.g. shard or role).
///
/// This algorithm uses a heuristic, and not the real Postgres parser, because the heuristic
/// is 2x faster and will work most of the time.
///
pub fn parse_edge_comment<'a>(
    query: &'a str,
    schema: &ShardingSchema,
) -> Result<QueryAndComment<'a>, Error> {
    let (after_leading, leading) = match leading_block_comment(query) {
        Some((rest, c)) => (rest, Some(c)),
        None => (query, None),
    };
    let (stripped, trailing) = match trailing_block_comment(after_leading) {
        Some((rest, c)) => (rest, Some(c)),
        None => (after_leading, None),
    };

    if leading.is_none() && trailing.is_none() {
        return Ok(QueryAndComment {
            query,
            #[cfg(test)]
            comment: String::new(),
            ..Default::default()
        });
    }

    // Leading wins per-field: extract from leading first, then fill in any
    // fields the leading didn't provide from trailing.
    let mut directives = match leading {
        Some(c) => directive::shard_role_from_comment(c, schema)?,
        None => Default::default(),
    };
    if let Some(c) = trailing {
        let trailing = directive::shard_role_from_comment(c, schema)?;
        if directives.shard.is_none() {
            directives.shard = trailing.shard;
            directives.sharding_key = trailing.sharding_key;
        }
        if directives.role.is_none() {
            directives.role = trailing.role;
        }
    }
    let (shard, role, sharding_key) = (directives.shard, directives.role, directives.sharding_key);

    Ok(QueryAndComment {
        query: stripped,
        #[cfg(test)]
        comment: match (leading, trailing) {
            (Some(l), Some(t)) => format!("{} {}", l, t),
            (Some(l), None) => l.to_string(),
            (None, Some(t)) => t.to_string(),
            (None, None) => String::new(),
        },
        shard,
        role,
        sharding_key,
    })
}
