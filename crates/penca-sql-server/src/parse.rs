//! SQL parsing utilities for the Flight SQL server.
//!
//! Sits between the gRPC transport ([`crate::flight_sql::service`]) and the
//! per-statement executors ([`crate::dml`], [`crate::tx`]). Parsing happens
//! once per request — the parsed AST is then dispatched to the right handler
//! based on the [`SqlStatement`] variant, avoiding the wasteful pattern of
//! parsing once for classification and again for execution.

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use tonic::Status;

/// Parse exactly one standard-SQL statement from `query`.
///
/// Errors on empty input, multiple statements, or DataFusion-specific
/// statement types (e.g. `CREATE EXTERNAL TABLE`) that aren't part of the
/// `sqlparser` AST. Used by the Flight SQL service layer to classify a
/// request as transaction-control vs DML before dispatching: the classifier
/// pattern-matches on the returned [`SqlStatement`], and the DML branch hands
/// the same value back to [`crate::dml::execute`] so we don't parse twice.
pub fn parse_one_statement(query: &str) -> Result<SqlStatement, Status> {
    let mut statements = DFParser::parse_sql(query)
        .map_err(|e| Status::invalid_argument(format!("failed to parse SQL: {e}")))?;
    let stmt = statements
        .pop_front()
        .ok_or_else(|| Status::invalid_argument("empty SQL statement"))?;
    if !statements.is_empty() {
        return Err(Status::invalid_argument(
            "multi-statement requests are not supported",
        ));
    }
    match stmt {
        DFStatement::Statement(boxed) => Ok(*boxed),
        _ => Err(Status::invalid_argument(
            "only standard SQL is supported on this endpoint",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty() {
        let err = parse_one_statement("").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn rejects_multi_statement() {
        let err = parse_one_statement("SELECT 1; SELECT 2").unwrap_err();
        assert!(
            err.message().contains("multi-statement"),
            "unexpected error: {}",
            err.message()
        );
    }

    #[test]
    fn parses_begin_as_start_transaction() {
        let stmt = parse_one_statement("BEGIN").unwrap();
        assert!(matches!(stmt, SqlStatement::StartTransaction { .. }));
    }

    #[test]
    fn parses_commit() {
        let stmt = parse_one_statement("COMMIT").unwrap();
        assert!(matches!(stmt, SqlStatement::Commit { .. }));
    }

    #[test]
    fn parses_rollback() {
        let stmt = parse_one_statement("ROLLBACK").unwrap();
        assert!(matches!(stmt, SqlStatement::Rollback { .. }));
    }

    #[test]
    fn parses_insert() {
        let stmt = parse_one_statement("INSERT INTO t VALUES (1)").unwrap();
        assert!(matches!(stmt, SqlStatement::Insert(_)));
    }
}
