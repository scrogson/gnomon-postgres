//! Branches: create, drop, rebase, list, and the connection options that put a session on one.

use serde_json::{Map, Value};

use crate::error::Result;
use crate::{Gnomon, bind, row_to_json};

/// Options for [`Branches::create`].
#[derive(Debug, Clone, Default)]
pub struct CreateBranch {
    /// Default `main`.
    pub parent: Option<String>,
    /// `logged` (default) or `scratch`.
    pub kind: Option<String>,
    /// Fork from a point in the parent's past (a time, `tag:x`, a coordinate).
    pub at: Option<String>,
    /// A PostgreSQL interval after which the branch is dropped.
    pub ttl: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Branches {
    g: Gnomon,
}

impl Branches {
    pub(crate) fn new(g: Gnomon) -> Self {
        Branches { g }
    }

    pub async fn create(&self, name: &str, opts: CreateBranch) -> Result<String> {
        self.g
            .app_rows(
                "select gnomon.create_branch($1, $2, $3, $4, $5::text::interval)",
                bind![
                    name,
                    opts.parent.as_deref().unwrap_or("main"),
                    opts.kind.as_deref().unwrap_or("logged"),
                    opts.at.as_deref(),
                    opts.ttl.as_deref(),
                ],
            )
            .await?;
        Ok(name.to_string())
    }

    /// A scratch branch with a generated name (for a test), forked from `parent`.
    pub async fn scratch(&self, prefix: &str, parent: &str) -> Result<String> {
        self.g
            .app_value(
                "select gnomon.create_scratch($1, $2)",
                bind![prefix, parent],
            )
            .await
    }

    /// Drop a branch and the branches forked from it.
    pub async fn drop(&self, name: &str) -> Result<()> {
        self.g
            .app_rows("select gnomon.drop_branch($1, true)::text", bind![name])
            .await?;
        Ok(())
    }

    /// Move a branch's base to main's present (`gnomon.rebase`).
    pub async fn rebase(&self, name: &str, dry_run: bool) -> Result<Value> {
        self.g
            .app_json(
                "select gnomon.rebase($1, null, $2)::text",
                bind![name, dry_run],
            )
            .await
    }

    /// The rows of `gnomon.branches`, oldest first.
    pub async fn list(&self) -> Result<Vec<Map<String, Value>>> {
        let rows = self
            .g
            .app_rows("select * from gnomon.branches order by created_at", bind![])
            .await?;
        rows.iter().map(row_to_json).collect()
    }

    /// The libpq `options` value whose sessions start on a branch (`gnomon.connection_options`).
    /// Pass `c_module` when the server loads gnomon's C module.
    pub async fn connection_options(&self, name: &str, c_module: bool) -> Result<String> {
        self.g
            .app_value(
                "select gnomon.connection_options($1, $2)",
                bind![name, c_module],
            )
            .await
    }
}
