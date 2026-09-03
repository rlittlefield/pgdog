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
    /// Copy only rows matching this WHERE predicate (MOVE KEYS copies
    /// one set of sharding keys; `broadcast_null` tables copy their
    /// NULL-key rows). Applies to the source side only: the
    /// destination COPY receives pre-filtered rows. `COPY (SELECT
    /// ...)` can't bind parameters, so the predicate arrives with its
    /// values already quoted.
    predicate: Option<String>,
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
            predicate: None,
        }
    }

    /// Copy out only the rows matching a WHERE predicate.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// Copy out only rows where the column is NULL (`broadcast_null`
    /// tables).
    pub fn with_null_filter(self, column: &str) -> Self {
        let predicate = format!(r#""{}" IS NULL"#, escape_identifier(column));
        self.with_predicate(predicate)
    }

    /// Generate COPY ... TO STDOUT statement.
    pub(crate) fn copy_out(&self) -> String {
        // `COPY table (...) TO STDOUT` doesn't accept a WHERE clause:
        // filtered copies use the query form.
        if let Some(predicate) = &self.predicate {
            return format!(
                r#"COPY (SELECT {} FROM "{}"."{}" WHERE {}) TO STDOUT WITH (FORMAT {})"#,
                self.quoted_columns(),
                self.schema_name(true),
                self.table_name(true),
                predicate,
                self.copy_format
            );
        }
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

    fn quoted_columns(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!(r#""{}""#, c))
            .collect::<Vec<_>>()
            .join(", ")
    }

    // Generate the statement.
    fn copy(&self, out: bool) -> String {
        format!(
            r#"COPY "{}"."{}" ({}) {} WITH (FORMAT {})"#,
            self.schema_name(out),
            self.table_name(out),
            self.quoted_columns(),
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

    #[test]
    fn test_copy_stmt_predicate() {
        let table = PublicationTable {
            schema: "public".into(),
            name: "orders".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(
            &table,
            &["id".into(), "tenant_id".into()],
            CopyFormat::Binary,
        )
        .with_predicate(r#""tenant_id" = ANY(ARRAY['11', '12'])"#);

        // The predicate filters the copy out.
        assert_eq!(
            copy.copy_out(),
            r#"COPY (SELECT "id", "tenant_id" FROM "public"."orders" WHERE "tenant_id" = ANY(ARRAY['11', '12'])) TO STDOUT WITH (FORMAT binary)"#
        );

        // The copy in is a plain table copy: the destination takes
        // every row the filtered copy out produced.
        assert_eq!(
            copy.copy_in(),
            r#"COPY "public"."orders" ("id", "tenant_id") FROM STDIN WITH (FORMAT binary)"#
        );
    }
}
