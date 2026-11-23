//! A Rust client for [Gnomon](https://gnomon.bitsinfra.com/): git for Postgres data, built for
//! agents. Branches, history, merges and undo inside PostgreSQL.
//!
//! Three layers, as in every gnomon SDK (the wire protocol is in `spec/SPEC.md`):
//!
//! - **Runs**: an agent's statements go to a branch of the database, inside an envelope that binds
//!   them to the run, attributes them and bounds them in time. The application reads what the run
//!   changed and merges it, submits it for review, or discards it.
//! - **Review**: change requests, their reviews and comments, merge and undo.
//! - **Core**: branches, history, blame, revert, diff, status.
//!
//! Every method is one SQL function call: the database is the only place with logic.
//!
//! ```no_run
//! # async fn demo() -> gnomon::Result<()> {
//! use gnomon::{Gnomon, StartRun};
//! use sqlx::PgPool;
//!
//! let app = PgPool::connect("postgres://shop_app@localhost/shop").await?;
//! let runs = PgPool::connect("postgres://shop_runs@localhost/shop").await?;
//! let gnomon = Gnomon::builder(app).runs(runs).build();
//!
//! let run = gnomon.start_run(StartRun::new().task("Refund order 3").actor("support-bot")).await?;
//! run.sql("update orders set status = 'refunded' where id = 3").await?;
//! let summary = run.summary().await?;
//! if summary.checks.passed && summary.changes.rows <= 20 {
//!     run.merge().await?;
//! } else {
//!     run.submit(Some("Refund order 3")).await?;
//! }
//! # Ok(()) }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{AssertSqlSafe, PgPool, Row};
use tokio::sync::OnceCell;

mod branches;
mod error;
mod json;
mod review;
mod run;
mod sqlsplit;
pub mod testing;
pub mod tools;

pub use branches::{Branches, CreateBranch};
pub use error::{Error, ErrorKind, Result, slug_of};
pub use json::{parse_json, row_to_json, row_to_json_lossy, stable_json};
pub use review::{CommentAt, Request, Requests};
pub use run::{MergeResult, Run, RunState, RunTx, SqlOptions, SqlResult, StartRun};
pub use sqlsplit::split_sql;

/// Re-exported so callers can build parameters for [`Run::sql_with`] without naming sqlx's paths.
pub use sqlx::postgres::PgArguments as Arguments;

/// The SQL API versions this crate speaks (`gnomon.api_version()`).
pub const SUPPORTED_API: std::ops::RangeInclusive<i32> = 2..=2;

/// Build [`PgArguments`] from values: `gnomon::args![1_i64, "x"]`.
#[macro_export]
macro_rules! args {
    ($($x:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut a = $crate::Arguments::default();
        $( $crate::__private::Arguments::add(&mut a, $x).expect("binding a parameter"); )*
        a
    }};
}

#[doc(hidden)]
pub mod __private {
    pub use sqlx::Arguments;
}

/// Internal: bind parameters, turning an encode failure into an error.
macro_rules! bind {
    ($($x:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut a = sqlx::postgres::PgArguments::default();
        $( sqlx::Arguments::add(&mut a, $x)
            .map_err(|e| $crate::Error::other(format!("binding a parameter: {e}")))?; )*
        a
    }};
}
pub(crate) use bind;

/// Which login a statement went out on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Login {
    App,
    Runs,
}

/// One statement, as the `on_statement` hook sees it.
#[derive(Debug)]
pub struct StatementEvent<'a> {
    pub run: Option<&'a str>,
    pub login: Login,
    pub statement: &'a str,
    pub duration: Duration,
    pub ok: bool,
    pub error: Option<&'a Error>,
    pub row_count: Option<u64>,
}

type Hook = Arc<dyn Fn(&StatementEvent<'_>) + Send + Sync>;

/// Settings for a [`Gnomon`]; set them through [`GnomonBuilder`].
#[derive(Clone)]
pub struct Options {
    /// The default `statement_timeout` of a run statement (30s).
    pub statement_timeout: Duration,
    /// The default `lock_timeout` of a run statement (5s).
    pub lock_timeout: Duration,
    /// Rows a run statement returns before `truncated` (1000).
    pub max_rows: usize,
    /// Skip the `gnomon.api_version()` check (tests of older servers).
    pub skip_version_check: bool,
    /// The database's connection string (host, port, database), for [`Run::url`].
    pub url: Option<String>,
    on_statement: Option<Hook>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            statement_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(5),
            max_rows: 1000,
            skip_version_check: false,
            url: None,
            on_statement: None,
        }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("statement_timeout", &self.statement_timeout)
            .field("lock_timeout", &self.lock_timeout)
            .field("max_rows", &self.max_rows)
            .field("skip_version_check", &self.skip_version_check)
            .field("url", &self.url.as_ref().map(|_| "<set>"))
            .field("on_statement", &self.on_statement.is_some())
            .finish()
    }
}

/// Builds a [`Gnomon`] from the application's pool and, for runs, the shared runs pool.
pub struct GnomonBuilder {
    app: PgPool,
    runs: Option<PgPool>,
    options: Options,
}

impl GnomonBuilder {
    /// The login every run shares: confined, no grant on the base tables. Needed for runs.
    pub fn runs(mut self, pool: PgPool) -> Self {
        self.runs = Some(pool);
        self
    }
    pub fn statement_timeout(mut self, d: Duration) -> Self {
        self.options.statement_timeout = d;
        self
    }
    pub fn lock_timeout(mut self, d: Duration) -> Self {
        self.options.lock_timeout = d;
        self
    }
    pub fn max_rows(mut self, n: usize) -> Self {
        self.options.max_rows = n;
        self
    }
    pub fn skip_version_check(mut self, yes: bool) -> Self {
        self.options.skip_version_check = yes;
        self
    }
    /// The base connection string [`Run::url`] puts a run's own login into.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.options.url = Some(url.into());
        self
    }
    /// Called after every statement, with its run, login, duration and outcome.
    pub fn on_statement(mut self, f: impl Fn(&StatementEvent<'_>) + Send + Sync + 'static) -> Self {
        self.options.on_statement = Some(Arc::new(f));
        self
    }
    pub fn build(self) -> Gnomon {
        Gnomon {
            inner: Arc::new(Inner {
                app: self.app,
                runs: self.runs,
                options: self.options,
                checked: OnceCell::new(),
            }),
        }
    }
}

struct Inner {
    app: PgPool,
    runs: Option<PgPool>,
    options: Options,
    checked: OnceCell<()>,
}

/// The client. Cheap to clone; clones share the pools.
#[derive(Clone)]
pub struct Gnomon {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Gnomon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gnomon")
            .field("runs", &self.inner.runs.is_some())
            .field("options", &self.inner.options)
            .finish()
    }
}

/// What kind of change a diff row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

/// One change between two references: an insert or delete (whole rows in `old`/`new`), or one
/// updated column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub table: String,
    pub key: Value,
    pub kind: ChangeKind,
    /// The column of an update; `None` for inserts and deletes.
    pub column: Option<String>,
    pub old: Value,
    pub new: Value,
}

/// `gnomon.run_summary`: changes, checks, the conflicts a merge would hit, and warnings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub version: i64,
    pub branch: String,
    pub status: String,
    pub changes: SummaryChanges,
    pub checks: SummaryChecks,
    pub conflicts: i64,
    #[serde(default)]
    pub warnings: Vec<Value>,
    pub behind_main: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryChanges {
    pub rows: i64,
    #[serde(default)]
    pub tables: Vec<TableChanges>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableChanges {
    pub table: String,
    pub inserts: i64,
    pub updates: i64,
    pub deletes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryChecks {
    pub passed: bool,
    pub passed_count: i64,
    pub failed_count: i64,
    #[serde(default)]
    pub results: Vec<CheckResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub check: String,
    pub passed: bool,
    pub detail: Option<String>,
}

/// A row of `gnomon.run_checks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Check {
    pub check_name: String,
    pub passed: bool,
    pub detail: Option<String>,
}

/// A row of `gnomon.doctor()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorCheck {
    pub check_name: String,
    pub status: String,
    pub detail: Option<String>,
}

/// Options for [`Gnomon::revert`].
#[derive(Debug, Clone, Default)]
pub struct Revert {
    pub keys: Option<Value>,
    pub filter: Option<String>,
    pub columns: Option<Vec<String>>,
    pub branch: Option<String>,
    pub dry_run: bool,
}

impl Gnomon {
    /// Start building a client on the application's own login (a member of `gnomon_runner`, and
    /// of `gnomon_merger` if your code merges into main).
    pub fn builder(app: PgPool) -> GnomonBuilder {
        GnomonBuilder {
            app,
            runs: None,
            options: Options::default(),
        }
    }

    /// A client with default options.
    pub fn new(app: PgPool, runs: Option<PgPool>) -> Gnomon {
        let b = Gnomon::builder(app);
        match runs {
            Some(r) => b.runs(r).build(),
            None => b.build(),
        }
    }

    pub fn options(&self) -> &Options {
        &self.inner.options
    }

    /// The application's pool.
    pub fn app_pool(&self) -> &PgPool {
        &self.inner.app
    }

    /// The runs pool, if one was given.
    pub fn runs_pool(&self) -> Option<&PgPool> {
        self.inner.runs.as_ref()
    }

    pub(crate) fn runs_or_err(&self) -> Result<&PgPool> {
        self.inner.runs.as_ref().ok_or_else(|| {
            Error::other("runs need the runs login: Gnomon::builder(app).runs(runs_pool)")
        })
    }

    /// The change requests layer: what a product's own Approve, Reject and Undo buttons call.
    pub fn requests(&self) -> Requests {
        Requests::new(self.clone())
    }

    /// The branches layer.
    pub fn branches(&self) -> Branches {
        Branches::new(self.clone())
    }

    /// Check once that the extension speaks this crate's API (spec: connect).
    pub async fn ready(&self) -> Result<()> {
        if self.inner.options.skip_version_check {
            return Ok(());
        }
        self.inner
            .checked
            .get_or_try_init(|| async {
                let v: i32 = sqlx::query_scalar("select gnomon.api_version()::int4")
                    .fetch_one(&self.inner.app)
                    .await
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::IncompatibleVersion,
                            "this server's gnomon has no gnomon.api_version(): it predates this client (needs API 2), or the extension is not installed",
                        )
                        .with_source(e)
                    })?;
                if !SUPPORTED_API.contains(&v) {
                    return Err(Error::new(
                        ErrorKind::IncompatibleVersion,
                        format!(
                            "gnomon API {v} on the server; this client speaks {}..={}",
                            SUPPORTED_API.start(),
                            SUPPORTED_API.end()
                        ),
                    ));
                }
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Report a statement to the hook and to `tracing`.
    pub(crate) fn report(
        &self,
        login: Login,
        run: Option<&str>,
        statement: &str,
        started: Instant,
        outcome: std::result::Result<Option<u64>, &Error>,
    ) {
        let duration = started.elapsed();
        match &outcome {
            Ok(_) => tracing::debug!(target: "gnomon", run, ?login, ?duration, "statement"),
            Err(e) => {
                tracing::debug!(target: "gnomon", run, ?login, ?duration, error = %e, "statement failed")
            }
        }
        if let Some(h) = &self.inner.options.on_statement {
            h(&StatementEvent {
                run,
                login,
                statement,
                duration,
                ok: outcome.is_ok(),
                error: outcome.err(),
                row_count: outcome.ok().flatten(),
            });
        }
    }

    /// Rows of a statement on the app login, with errors typed and the hook called.
    pub(crate) async fn app_rows(
        &self,
        sql: &'static str,
        args: PgArguments,
    ) -> Result<Vec<PgRow>> {
        self.ready().await?;
        let t0 = Instant::now();
        let r = sqlx::query_with(sql, args)
            .fetch_all(&self.inner.app)
            .await
            .map_err(Error::from);
        self.report(
            Login::App,
            None,
            sql,
            t0,
            r.as_ref().map(|x| Some(x.len() as u64)),
        );
        r
    }

    /// The first column of the first row.
    pub(crate) async fn app_value<T>(&self, sql: &'static str, args: PgArguments) -> Result<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    {
        let rows = self.app_rows(sql, args).await?;
        let row = rows
            .first()
            .ok_or_else(|| Error::other(format!("no row from {sql}")))?;
        Ok(row.try_get(0)?)
    }

    /// A jsonb result, selected as text and parsed losslessly. `sql` must yield one text column.
    pub(crate) async fn app_json(&self, sql: &'static str, args: PgArguments) -> Result<Value> {
        let t: Option<String> = self.app_value(sql, args).await?;
        parse_json(t.as_deref())
    }

    pub(crate) async fn app_json_as<T: serde::de::DeserializeOwned>(
        &self,
        sql: &'static str,
        args: PgArguments,
    ) -> Result<T> {
        Ok(serde_json::from_value(self.app_json(sql, args).await?)?)
    }

    /// Run one statement of your own on the application login. Rows come back as sqlx rows; see
    /// [`row_to_json`] for maps.
    pub async fn app_sql(&self, statement: &str, args: PgArguments) -> Result<Vec<PgRow>> {
        self.ready().await?;
        let t0 = Instant::now();
        let r = sqlx::query_with(AssertSqlSafe(statement.to_string()), args)
            .fetch_all(&self.inner.app)
            .await
            .map_err(Error::from);
        self.report(
            Login::App,
            None,
            statement,
            t0,
            r.as_ref().map(|x| Some(x.len() as u64)),
        );
        r
    }

    /// Start an agent run: a branch the runs login may write (`gnomon.run_start`).
    pub async fn start_run(&self, opts: StartRun) -> Result<Run> {
        self.runs_or_err()?;
        let branch: String = self
            .app_value(
                "select gnomon.run_start($1, $2, $3, $4::text::interval, $5)",
                bind![
                    opts.task.as_deref(),
                    opts.actor.as_deref(),
                    opts.kind.as_deref().unwrap_or("logged"),
                    opts.ttl.as_deref(),
                    opts.parent.as_deref().unwrap_or("main"),
                ],
            )
            .await?;
        Ok(Run::new(self.clone(), branch, &opts))
    }

    /// Start a run, call `f` with it, and discard it unless `f` merged or submitted it, whether
    /// `f` succeeded or not. Rust has no async drop: this is the scoped form of a run. If `f`
    /// panics, the run is left open until its TTL.
    pub async fn with_run<F, Fut, T>(&self, opts: StartRun, f: F) -> Result<T>
    where
        F: FnOnce(Run) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let run = self.start_run(opts).await?;
        let out = f(run.clone()).await;
        if run.state() == RunState::Open {
            let d = run.discard().await;
            if out.is_ok() {
                d?;
            }
        }
        out
    }

    /// A run already started (by its branch name), e.g. in another process.
    pub fn run(&self, branch: impl Into<String>, opts: &StartRun) -> Run {
        Run::new(self.clone(), branch.into(), opts)
    }

    // ---- core

    /// `gnomon.status()`: where this session is and how.
    pub async fn status(&self) -> Result<Value> {
        self.app_json("select gnomon.status()::text", bind![]).await
    }

    /// `gnomon.doctor()`.
    pub async fn doctor(&self) -> Result<Vec<DoctorCheck>> {
        let rows = self
            .app_rows(
                "select check_name, status, detail from gnomon.doctor()",
                bind![],
            )
            .await?;
        rows.iter()
            .map(|r| {
                Ok(DoctorCheck {
                    check_name: r.try_get(0)?,
                    status: r.try_get(1)?,
                    detail: r.try_get(2)?,
                })
            })
            .collect()
    }

    /// `gnomon.describe(schema)`: tables, columns, keys, foreign keys, what may be changed.
    pub async fn describe(&self, schema: Option<&str>) -> Result<Value> {
        self.app_json("select gnomon.describe($1)::text", bind![schema])
            .await
    }

    /// The versions of one row (`gnomon.history`), oldest first, as JSON objects with `vkey`,
    /// `xid`, `role`, `op`, `at`, `actor`, `reason`, `row_image` and `on_branch`.
    pub async fn history(
        &self,
        table: &str,
        key: &Value,
        branch: Option<&str>,
    ) -> Result<Vec<Value>> {
        self.app_json_as(
            "select coalesce(jsonb_agg(to_jsonb(h)), '[]')::text from gnomon.history($1::text::regclass, $2::text::jsonb, $3) h",
            bind![table, key.to_string(), branch.unwrap_or("main")],
        )
        .await
    }

    /// Who last set each field of one row (`gnomon.blame`), as JSON objects.
    pub async fn blame(
        &self,
        table: &str,
        key: &Value,
        branch: Option<&str>,
    ) -> Result<Vec<Value>> {
        self.app_json_as(
            "select coalesce(jsonb_agg(to_jsonb(b)), '[]')::text from gnomon.blame($1::text::regclass, $2::text::jsonb, $3) b",
            bind![table, key.to_string(), branch.unwrap_or("main")],
        )
        .await
    }

    /// Put rows of a table back as they were at a reference (`gnomon.revert`).
    pub async fn revert(&self, table: &str, at: &str, opts: Revert) -> Result<Value> {
        self.app_json(
            "select gnomon.revert($1::text::regclass, $2, $3::text::jsonb, $4, $5, $6, $7)::text",
            bind![
                table,
                at,
                opts.keys.as_ref().map(|k| k.to_string()),
                opts.filter.as_deref(),
                opts.columns.as_deref(),
                opts.branch.as_deref().unwrap_or("main"),
                opts.dry_run,
            ],
        )
        .await
    }

    /// Changes between two references (a branch, main, `branch@ref`, a coordinate, `tag:x`, a
    /// time). With `b` omitted, a branch against where it forked.
    pub async fn diff(&self, a: &str, b: Option<&str>) -> Result<Vec<Change>> {
        let rows = self
            .app_rows(
                "select tbl::text, key::text, kind, col, old::text, new::text from gnomon.diff($1, $2)",
                bind![a, b],
            )
            .await?;
        rows.iter()
            .map(|r| {
                let kind: String = r.try_get(2)?;
                Ok(Change {
                    table: r.try_get(0)?,
                    key: parse_json(r.try_get::<Option<&str>, _>(1)?)?,
                    kind: match kind.as_str() {
                        "insert" => ChangeKind::Insert,
                        "update" => ChangeKind::Update,
                        "delete" => ChangeKind::Delete,
                        k => return Err(Error::other(format!("unknown change kind {k}"))),
                    },
                    column: r.try_get(3)?,
                    old: parse_json(r.try_get::<Option<&str>, _>(4)?)?,
                    new: parse_json(r.try_get::<Option<&str>, _>(5)?)?,
                })
            })
            .collect()
    }

    /// What a gnomon error means and what to do about it (`gnomon.explain_error`); `None` when
    /// gnomon has nothing to say about it.
    pub async fn explain_error(&self, err: &Error) -> Result<Option<String>> {
        let message = match err.hint() {
            Some(h) => format!("{}\n{h}", err.message()),
            None => err.message().to_string(),
        };
        self.app_value(
            "select gnomon.explain_error($1, $2)",
            bind![err.sqlstate(), message],
        )
        .await
    }
}
