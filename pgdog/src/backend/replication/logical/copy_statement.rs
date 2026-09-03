//!
//! Generate COPY statement for table synchronization.
//!

use pgdog_config::CopyFormat;

use super::publisher::PublicationTable;
use crate::util::escape_identifier;

/// COPY statement generator.
#[derive(Debug, Clone)]
pub(crate) struct CopyStatement {
    table: PublicationTable,
    columns: Vec<String>,
    copy_format: CopyFormat,
    /// Copy only rows where this column is NULL (`broadcast_null`
    /// tables). Applies to the source side only: the destination COPY
    /// receives pre-filtered rows.
    null_filter_column: Option<String>,
}

impl CopyStatement {
    /// Create new COPY statement generator.
    ///
    /// # Arguments
    ///
    /// * `schema`: Name of the schema.
    /// * `table`: Name of the table.
    /// * `columns`: Table column names.
    ///
    pub(crate) fn new(
        table: &PublicationTable,
        columns: &[String],
        copy_format: CopyFormat,
    ) -> CopyStatement {
        CopyStatement {
            table: table.clone(),
            columns: columns.to_vec(),
            copy_format,
            null_filter_column: None,
        }
    }

    /// Copy only rows where the column is NULL (source side).
    pub fn with_null_filter(mut self, column: &str) -> Self {
        self.null_filter_column = Some(column.to_string());
        self
    }

    /// Generate COPY ... TO STDOUT statement.
    pub(crate) fn copy_out(&self) -> String {
        self.copy(true)
    }

    /// Generate COPY ... FROM STDIN statement.
    pub(crate) fn copy_in(&self) -> String {
        self.copy(false)
    }

    fn schema_name(&self, out: bool) -> &str {
        if out || self.table.parent_schema.is_empty() {
            &self.table.schema
        } else {
            &self.table.parent_schema
        }
    }

    fn table_name(&self, out: bool) -> &str {
        if out || self.table.parent_name.is_empty() {
            &self.table.name
        } else {
            &self.table.parent_name
        }
    }

    // Generate the statement.
    fn copy(&self, out: bool) -> String {
        let columns = self
            .columns
            .iter()
            .map(|c| format!(r#""{}""#, c))
            .collect::<Vec<_>>()
            .join(", ");

        // `COPY table (...) TO STDOUT` doesn't accept a WHERE clause:
        // filtered copies use the query form.
        if out && let Some(column) = &self.null_filter_column {
            return format!(
                r#"COPY (SELECT {} FROM "{}"."{}" WHERE "{}" IS NULL) TO STDOUT WITH (FORMAT {})"#,
                columns,
                self.schema_name(out),
                self.table_name(out),
                escape_identifier(column),
                self.copy_format
            );
        }

        format!(
            r#"COPY "{}"."{}" ({}) {} WITH (FORMAT {})"#,
            self.schema_name(out),
            self.table_name(out),
            columns,
            if out { "TO STDOUT" } else { "FROM STDIN" },
            self.copy_format
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_copy_stmt() {
        let table = PublicationTable {
            schema: "public".into(),
            name: "test".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &["id".into(), "email".into()], CopyFormat::Binary);
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(),
            r#"COPY "public"."test" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );

        let table = PublicationTable {
            schema: "public".into(),
            name: "test_0".into(),
            parent_name: "test".into(),
            parent_schema: "public".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &["id".into(), "email".into()], CopyFormat::Binary);
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(),
            r#"COPY "public"."test_0" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );
    }

    #[test]
    fn test_copy_stmt_null_filter() {
        let table = PublicationTable {
            schema: "public".into(),
            name: "packages".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &["id".into(), "org_id".into()], CopyFormat::Binary)
            .with_null_filter("org_id");

        // Source side: the query form carries the filter.
        assert_eq!(
            copy.copy_out(),
            r#"COPY (SELECT "id", "org_id" FROM "public"."packages" WHERE "org_id" IS NULL) TO STDOUT WITH (FORMAT binary)"#
        );

        // Destination side receives pre-filtered rows: plain form.
        assert_eq!(
            copy.copy_in(),
            r#"COPY "public"."packages" ("id", "org_id") FROM STDIN WITH (FORMAT binary)"#
        );
    }
}
