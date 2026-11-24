//! Review: change requests, their reviews and comments, merge and undo. What a product's own
//! Approve, Reject and Undo buttons call.

use std::time::Duration;

use serde_json::Value;
use sqlx::Row;

use crate::error::{Error, Result};
use crate::{Change, Gnomon, bind};

/// The change requests of a database.
#[derive(Debug, Clone)]
pub struct Requests {
    g: Gnomon,
}

impl Requests {
    pub(crate) fn new(g: Gnomon) -> Self {
        Requests { g }
    }

    /// Change requests still open or approved (not merged or closed), oldest first.
    pub async fn open(&self) -> Result<Vec<Request>> {
        let rows = self
            .g
            .app_rows("select id from gnomon.change_request where status in ('open', 'approved') order by id", bind![])
            .await?;
        rows.iter()
            .map(|r| Ok(Request::new(self.g.clone(), r.try_get(0)?)))
            .collect()
    }

    pub fn get(&self, id: i64) -> Request {
        Request::new(self.g.clone(), id)
    }
}

/// Where a comment points: a table, a row's key, a column. All optional.
#[derive(Debug, Clone, Default)]
pub struct CommentAt {
    pub table: Option<String>,
    pub key: Option<Value>,
    pub column: Option<String>,
}

/// One change request.
#[derive(Debug, Clone)]
pub struct Request {
    g: Gnomon,
    id: i64,
}

impl Request {
    pub(crate) fn new(g: Gnomon, id: i64) -> Self {
        Request { g, id }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    /// `gnomon.change_request_status`: the request, its branch, reviews, checks and merge.
    pub async fn status(&self) -> Result<Value> {
        self.g
            .app_json(
                "select gnomon.change_request_status($1)::text",
                bind![self.id],
            )
            .await
    }

    async fn branch(&self) -> Result<String> {
        let s = self.status().await?;
        s.get("branch")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                Error::other(format!(
                    "change request {} has no branch in its status",
                    self.id
                ))
            })
    }

    /// What the request would change.
    pub async fn diff(&self) -> Result<Vec<Change>> {
        let b = self.branch().await?;
        self.g.diff(&b, None).await
    }

    pub async fn approve(&self, body: Option<&str>) -> Result<()> {
        self.g
            .app_rows(
                "select gnomon.review($1, 'approve', $2)::text",
                bind![self.id, body],
            )
            .await?;
        Ok(())
    }

    pub async fn reject(&self, note: Option<&str>) -> Result<()> {
        self.g
            .app_rows(
                "select gnomon.review($1, 'reject', $2)::text",
                bind![self.id, note],
            )
            .await?;
        Ok(())
    }

    /// Comment on the request, or on one table, row or field of it. Returns the comment's id.
    pub async fn comment(&self, body: &str, at: CommentAt) -> Result<i64> {
        self.g
            .app_value(
                "select gnomon.comment($1, $2, $3::text::regclass, $4::text::jsonb, $5)::int8",
                bind![
                    self.id,
                    body,
                    at.table.as_deref(),
                    at.key.as_ref().map(|k| k.to_string()),
                    at.column.as_deref()
                ],
            )
            .await
    }

    /// Merge the request's branch and close it. Returns the merge's id.
    /// Conflicts are an error of kind [`ErrorKind::MergeConflict`](crate::ErrorKind), as for
    /// [`Run::merge`](crate::Run::merge); so is any merge that did not apply.
    pub async fn merge(&self) -> Result<i64> {
        let b = self.branch().await?;
        Ok(crate::run::merge_branch(&self.g, &b, true).await?.merge)
    }

    /// Undo the request's merge (`gnomon.undo_merge`).
    pub async fn undo(&self) -> Result<Value> {
        let s = self.status().await?;
        let merge_id = s
            .get("merge")
            .and_then(|m| m.get("id"))
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::other(format!("change request {} is not merged", self.id)))?;
        self.g
            .app_json("select gnomon.undo_merge($1)::text", bind![merge_id])
            .await
    }

    /// Wait until a person reviews the request, or the timeout passes (`gnomon.wait_for_review`).
    pub async fn wait(&self, timeout: Duration) -> Result<Value> {
        self.g
            .app_json(
                "select gnomon.wait_for_review($1, $2::text::interval)::text",
                bind![self.id, format!("{} milliseconds", timeout.as_millis())],
            )
            .await
    }
}
