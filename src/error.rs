//! Typed errors (spec: Errors). The SQLSTATE stays the standard one; the slug in the hint
//! (`[gnomon:<slug>]`) is gnomon's stable code.

use std::fmt;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

/// What a caller branches on. Mirrors the error classes every gnomon SDK exposes.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Any other gnomon or database error (`GnomonError`).
    Other,
    /// SQLSTATE 40001: retry the transaction.
    Serialization,
    /// SQLSTATE 42501.
    Permission,
    /// SQLSTATE 25006, or slug `branch_closed`.
    BranchClosed,
    /// SQLSTATE 57014.
    StatementTimeout,
    /// SQLSTATE 42P10 (an upsert on a branch), or slug `sql_only_dml` / `upsert_on_branch`.
    UnsupportedOnBranch,
    /// Slug `not_routed`.
    NotRouted,
    /// A merge stopped on conflicts.
    MergeConflict {
        merge_id: i64,
        conflicts: Vec<Value>,
    },
    /// SQLSTATE 23514: a check failed inside a merge.
    ChecksFailed,
    /// SQLSTATE 53400, or slug `over_quota`.
    OverQuota,
    /// The extension's API version is outside the range this crate speaks.
    IncompatibleVersion,
}

impl ErrorKind {
    /// The class name the other SDKs and the conformance scenarios use.
    pub fn class_name(&self) -> &'static str {
        match self {
            ErrorKind::Other => "GnomonError",
            ErrorKind::Serialization => "SerializationError",
            ErrorKind::Permission => "PermissionError",
            ErrorKind::BranchClosed => "BranchClosedError",
            ErrorKind::StatementTimeout => "StatementTimeoutError",
            ErrorKind::UnsupportedOnBranch => "UnsupportedOnBranchError",
            ErrorKind::NotRouted => "NotRoutedError",
            ErrorKind::MergeConflict { .. } => "MergeConflictError",
            ErrorKind::ChecksFailed => "ChecksFailedError",
            ErrorKind::OverQuota => "OverQuotaError",
            ErrorKind::IncompatibleVersion => "IncompatibleVersionError",
        }
    }
}

/// A gnomon error: the kind, gnomon's stable `code` (slug), the SQLSTATE, the hint, and the
/// driver's error as the source. Messages are written for a model to read.
pub struct Error(Box<Inner>);

#[derive(Debug)]
struct Inner {
    kind: ErrorKind,
    message: String,
    code: Option<String>,
    sqlstate: Option<String>,
    hint: Option<String>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error(Box::new(Inner {
            kind,
            message: message.into(),
            code: None,
            sqlstate: None,
            hint: None,
            source: None,
        }))
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// The slug in the hint, `[gnomon:<slug>]` (see <https://gnomon.bitsinfra.com/errors/>).
    pub fn code(&self) -> Option<&str> {
        self.0.code.as_deref()
    }

    pub fn sqlstate(&self) -> Option<&str> {
        self.0.sqlstate.as_deref()
    }

    pub fn hint(&self) -> Option<&str> {
        self.0.hint.as_deref()
    }

    pub(crate) fn other(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Other, message)
    }

    pub(crate) fn with_code(mut self, code: &str) -> Self {
        self.0.code = Some(code.to_string());
        self
    }

    pub(crate) fn with_sqlstate(mut self, s: &str) -> Self {
        self.0.sqlstate = Some(s.to_string());
        self
    }

    pub(crate) fn with_source(mut self, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.0.source = Some(Box::new(e));
        self
    }

    pub fn is_serialization(&self) -> bool {
        self.0.kind == ErrorKind::Serialization
    }

    /// The conflicts of a merge that stopped on them, with the merge's id.
    pub fn merge_conflicts(&self) -> Option<(i64, &[Value])> {
        match &self.0.kind {
            ErrorKind::MergeConflict {
                merge_id,
                conflicts,
            } => Some((*merge_id, conflicts)),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0.kind.class_name(), self.0.message)?;
        if let Some(h) = &self.0.hint {
            write!(f, "\nhint: {h}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

static SLUG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[gnomon:([a-z_]+)\]").unwrap());

/// The `[gnomon:<slug>]` in a hint or message, if any.
pub fn slug_of(text: &str) -> Option<String> {
    SLUG.captures(text).map(|c| c[1].to_string())
}

/// Classify an error by SQLSTATE and slug, in the same precedence order as every gnomon SDK.
pub(crate) fn classify(sqlstate: Option<&str>, slug: Option<&str>) -> ErrorKind {
    match (sqlstate, slug) {
        (Some("40001"), _) => ErrorKind::Serialization,
        (Some("57014"), _) => ErrorKind::StatementTimeout,
        (Some("42P10"), _) | (_, Some("sql_only_dml" | "upsert_on_branch")) => {
            ErrorKind::UnsupportedOnBranch
        }
        (_, Some("not_routed")) => ErrorKind::NotRouted,
        (Some("25006"), _) | (_, Some("branch_closed")) => ErrorKind::BranchClosed,
        (Some("53400"), _) | (_, Some("over_quota")) => ErrorKind::OverQuota,
        (Some("23514"), _) => ErrorKind::ChecksFailed,
        (Some("42501"), _) => ErrorKind::Permission,
        _ => ErrorKind::Other,
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        let (message, sqlstate, hint) = match &e {
            sqlx::Error::Database(db) => {
                let hint = db
                    .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                    .and_then(|p| p.hint().map(str::to_string));
                (
                    db.message().to_string(),
                    db.code().map(|c| c.into_owned()),
                    hint,
                )
            }
            other => (other.to_string(), None, None),
        };
        let slug = slug_of(&format!("{} {}", hint.as_deref().unwrap_or(""), message));
        let kind = classify(sqlstate.as_deref(), slug.as_deref());
        let message = if kind == ErrorKind::UnsupportedOnBranch {
            format!(
                "{message} (on a branch, write an upsert as UPDATE then INSERT ... WHERE NOT EXISTS, or load gnomon's C module)"
            )
        } else {
            message
        };
        Error(Box::new(Inner {
            kind,
            message,
            code: slug,
            sqlstate,
            hint,
            source: Some(Box::new(e)),
        }))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::other(format!("invalid JSON: {e}")).with_source(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence() {
        assert_eq!(
            classify(Some("40001"), Some("branch_closed")),
            ErrorKind::Serialization
        );
        assert_eq!(
            classify(Some("42501"), Some("not_routed")),
            ErrorKind::NotRouted
        );
        assert_eq!(
            classify(Some("XX000"), Some("sql_only_dml")),
            ErrorKind::UnsupportedOnBranch
        );
        assert_eq!(classify(Some("42501"), None), ErrorKind::Permission);
        assert_eq!(classify(None, None), ErrorKind::Other);
    }

    #[test]
    fn slug() {
        assert_eq!(
            slug_of("try again [gnomon:branch_closed]").as_deref(),
            Some("branch_closed")
        );
        assert_eq!(slug_of("nothing here"), None);
    }
}
