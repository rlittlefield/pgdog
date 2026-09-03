//! Move sharding keys to another shard.
//!
//! `MOVE KEYS <database> <target_shard> <key>[,<key>...] [AUTO]`
//! launches the [`MoveKeysTask`]: copy the keys' rows to the target
//! shard, catch up over logical replication, and flip their placement
//! with the configured `move_query`, on `CUTOVER <database>
//! <target_shard>` or automatically with `AUTO`. Keys are
//! comma-separated; single quotes preserve case, spaces and commas,
//! with `''` for a literal quote. Progress in `SHOW TASKS`, abort with
//! `STOP_TASK`.
//!
//! Unlike the other admin commands, the keys are values: this command
//! parses the original, case-preserved input.

use tracing::info;

use crate::api::move_keys::MoveKeysTask;
use crate::api::run_task;
use crate::backend::databases::databases;

use super::prelude::*;

/// Move sharding keys to another shard.
pub struct MoveKeys {
    database: String,
    target: usize,
    keys: Vec<String>,
    auto_cutover: bool,
}

/// Split a comma-separated key list. Quoted items (`'...'`, `''`
/// escapes a quote) preserve anything; bare items run to the next
/// comma or whitespace. Returns the keys and whatever followed the
/// list (e.g. `AUTO`).
fn parse_keys(input: &str) -> Result<(Vec<String>, &str), Error> {
    let mut keys = vec![];
    let mut rest = input.trim_start();

    loop {
        if let Some(quoted) = rest.strip_prefix('\'') {
            // Quoted key: scan for the closing quote, unescaping ''.
            let mut key = String::new();
            let mut close = None;
            let mut chars = quoted.char_indices().peekable();
            while let Some((position, c)) = chars.next() {
                if c != '\'' {
                    key.push(c);
                } else if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                    key.push('\'');
                } else {
                    close = Some(position + 1);
                    break;
                }
            }
            // Unterminated quote.
            let Some(close) = close else {
                return Err(Error::Syntax);
            };
            keys.push(key);
            rest = quoted[close..].trim_start();
        } else {
            // Bare key: up to a comma or whitespace.
            let end = rest.find([',', ' ', '\t']).unwrap_or(rest.len());
            let key = &rest[..end];
            if key.is_empty() {
                return Err(Error::Syntax);
            }
            keys.push(key.to_string());
            rest = rest[end..].trim_start();
        }

        match rest.strip_prefix(',') {
            Some(after) => rest = after.trim_start(),
            None => break,
        }
    }

    Ok((keys, rest.trim()))
}

#[async_trait]
impl Command for MoveKeys {
    /// Parses the original, case-preserved input: keys are values.
    fn parse(sql: &str) -> Result<Self, Error> {
        let mut words = sql.trim().splitn(5, char::is_whitespace);

        let (Some(move_kw), Some(keys_kw), Some(database), Some(target), Some(list)) = (
            words.next(),
            words.next(),
            words.next(),
            words.next(),
            words.next(),
        ) else {
            return Err(Error::Syntax);
        };
        if !move_kw.eq_ignore_ascii_case("move") || !keys_kw.eq_ignore_ascii_case("keys") {
            return Err(Error::Syntax);
        }

        let target = target.parse().map_err(|_| Error::Syntax)?;
        let (keys, rest) = parse_keys(list)?;

        let auto_cutover = match rest {
            "" => false,
            auto if auto.eq_ignore_ascii_case("auto") => true,
            _ => return Err(Error::Syntax),
        };

        Ok(Self {
            database: database.to_string(),
            target,
            keys,
            auto_cutover,
        })
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        info!(
            r#"moving {} key(s) of "{}" to shard {}"#,
            self.keys.len(),
            self.database,
            self.target
        );

        // Cheap validation now for an immediate error on a missing
        // database or shard; the deeper guards run inside the task and
        // surface in SHOW TASKS.
        let cluster = databases().schema_owner(&self.database)?;
        if self.target >= cluster.shards().len() {
            return Err(Error::Syntax);
        }

        let task_id = run_task(
            MoveKeysTask::builder()
                .database(self.database.clone())
                .target(self.target)
                .keys(self.keys.clone())
                .auto_cutover(self.auto_cutover)
                .build(),
        )
        .id();

        let mut dr = DataRow::new();
        dr.add(task_id.to_string());

        Ok(vec![
            RowDescription::new(&[Field::text("task_id")]).message(),
            dr.message(),
        ])
    }

    fn name(&self) -> String {
        "MOVE KEYS".into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_move_keys() {
        let cmd = MoveKeys::parse("MOVE KEYS prod 2 11,12").unwrap();
        assert_eq!(cmd.database, "prod");
        assert_eq!(cmd.target, 2);
        assert_eq!(cmd.keys, vec!["11", "12"]);
        assert!(!cmd.auto_cutover);

        // Spaces around commas, AUTO, and mixed keyword case.
        let cmd = MoveKeys::parse("move keys prod 2 11, 12 AUTO").unwrap();
        assert_eq!(cmd.keys, vec!["11", "12"]);
        assert!(cmd.auto_cutover);

        // Quoted keys preserve case, spaces, commas, and quotes.
        let cmd = MoveKeys::parse("MOVE KEYS prod 1 'Acme Corp','O''Brien, Inc'").unwrap();
        assert_eq!(cmd.keys, vec!["Acme Corp", "O'Brien, Inc"]);

        // Quoted and bare keys mix.
        let cmd = MoveKeys::parse("MOVE KEYS prod 1 'Acme Corp', 42 auto").unwrap();
        assert_eq!(cmd.keys, vec!["Acme Corp", "42"]);
        assert!(cmd.auto_cutover);

        // UUIDs keep their case for canonicalization downstream.
        let cmd = MoveKeys::parse("MOVE KEYS prod 1 550E8400-E29B-41D4-A716-446655440000").unwrap();
        assert_eq!(cmd.keys, vec!["550E8400-E29B-41D4-A716-446655440000"]);

        // Malformed.
        assert!(MoveKeys::parse("MOVE KEYS prod 2").is_err());
        assert!(MoveKeys::parse("MOVE KEYS prod two 11").is_err());
        assert!(MoveKeys::parse("MOVE KEYS prod 2 11,").is_err());
        assert!(MoveKeys::parse("MOVE KEYS prod 2 'unterminated").is_err());
        assert!(MoveKeys::parse("MOVE KEYS prod 2 11 12").is_err());
        assert!(MoveKeys::parse("MOVE SHARD prod 2 11").is_err());
    }
}
