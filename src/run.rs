//! Runs: one agent task on a branch of its own, behind the envelope (spec: The envelope).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{AssertSqlSafe, Connection, Either, Executor, PgConnection, Postgres, Row, Transaction};

use crate::error::{Error, ErrorKind, Result};
use crate::json::{parse_json, row_to_json};
use crate::review::Request;
use crate::sqlsplit::split_sql;
use crate::{Change, Check, Gnomon, Login, Summary, bind};

/// Options for [`Gnomon::start_run`].
#[derive(Debug, Clone, Default)]
pub struct StartRun {
    pub task: Option<String>,
    pub actor: Option<String>,
    /// `logged` (default) or `scratch`.
    pub kind: Option<String>,
    /// How long the run's branch lives, as a PostgreSQL interval (`'2 hours'`).
    pub ttl: Option<String>,
    /// The branch the run forks from (default `main`).
    pub parent: Option<String>,
    /// A wall-clock limit for the whole run.
    pub deadline: Option<Duration>,
}

impl StartRun {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn task(mut self, t: impl Into<String>) -> Self {
        self.task = Some(t.into());
        self
    }
    pub fn actor(mut self, a: impl Into<String>) -> Self {
        self.actor = Some(a.into());
        self
    }
    pub fn scratch(mut self) -> Self {
        self.kind = Some("scratch".into());
        self
    }
    pub fn ttl(mut self, interval: impl Into<String>) -> Self {
        self.ttl = Some(interval.into());
        self
    }
    pub fn parent(mut self, p: impl Into<String>) -> Self {
        self.parent = Some(p.into());
        self
    }
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = Some(d);
        self
    }
}

/// Per-statement options for [`Run::sql_opts`].
#[derive(Debug, Clone, Default)]
pub struct SqlOptions {
    /// Why, for writes: kept with the change (history, blame, the log).
    pub reason: Option<String>,
    pub timeout: Option<Duration>,
    pub max_rows: Option<usize>,
}

/// The rows of one run statement. Rows past the cap are dropped and `truncated` says so.
#[derive(Debug)]
pub struct SqlResult {
    pub rows: Vec<PgRow>,
    /// Rows affected (writes) or returned (reads), before the cap.
    pub row_count: u64,
    pub fields: Vec<String>,
    pub truncated: bool,
}

impl SqlResult {
    /// The rows as JSON objects (see [`row_to_json`] for the types covered).
    pub fn json_rows(&self) -> Result<Vec<Map<String, Value>>> {
        self.rows.iter().map(row_to_json).collect()
    }
}

/// Where a run stands, as far as this client knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Open,
    Merged,
    Submitted,
    Discarded,
}

/// The result of [`Run::merge`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeResult {
    pub merge: i64,
    pub status: String,
    pub stats: Value,
}

struct RunInner {
    branch: String,
    task: Option<String>,
    actor: Option<String>,
    seq: AtomicU64,
    state: Mutex<RunState>,
    deadline: Option<Instant>,
}

/// One agent run: a branch, its envelope, and what the application does with it. Cheap to clone.
///
/// Rust has no async drop, so a run dropped while open stays open until its TTL: use
/// [`Gnomon::with_run`], or call [`Run::discard`].
#[derive(Clone)]
pub struct Run {
    g: Gnomon,
    inner: Arc<RunInner>,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run")
            .field("branch", &self.inner.branch)
            .field("state", &self.state())
            .finish()
    }
}

const RETRIES: u32 = 3;

/// Whole milliseconds, rounded up and at least 1: PostgreSQL reads a timeout of 0 as no limit.
fn ceil_ms(d: Duration) -> u128 {
    d.as_micros().div_ceil(1000).max(1)
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(20 * 2u64.pow(attempt))).await;
}

fn one_statement(statement: &str, hint: &str) -> Result<()> {
    if split_sql(statement).len() > 1 {
        return Err(Error::other(format!(
            "one statement per call: this one holds several ({hint})"
        ))
        .with_code("one_statement")
        .with_sqlstate("42601"));
    }
    Ok(())
}

/// Send one statement by the extended protocol and keep its rows and its row count.
async fn exec_one(
    conn: &mut PgConnection,
    statement: &str,
    args: PgArguments,
    cap: usize,
) -> Result<SqlResult> {
    let q = sqlx::query_with(AssertSqlSafe(statement.to_string()), args).persistent(false);
    let mut stream = conn.fetch_many(q);
    let mut rows = Vec::new();
    let mut row_count = 0u64;
    let mut seen = 0usize;
    let mut fields: Option<Vec<String>> = None;
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(done) => row_count += done.rows_affected(),
            Either::Right(row) => {
                if fields.is_none() {
                    fields = Some(
                        sqlx::Row::columns(&row)
                            .iter()
                            .map(|c| sqlx::Column::name(c).to_string())
                            .collect(),
                    );
                }
                seen += 1;
                if rows.len() < cap {
                    rows.push(row);
                }
            }
        }
    }
    Ok(SqlResult {
        rows,
        row_count,
        fields: fields.unwrap_or_default(),
        truncated: seen > cap,
    })
}

impl Run {
    pub(crate) fn new(g: Gnomon, branch: String, opts: &StartRun) -> Run {
        Run {
            g,
            inner: Arc::new(RunInner {
                branch,
                task: opts.task.clone(),
                actor: opts.actor.clone(),
                seq: AtomicU64::new(0),
                state: Mutex::new(RunState::Open),
                deadline: opts.deadline.map(|d| Instant::now() + d),
            }),
        }
    }

    /// The run's branch name.
    pub fn branch(&self) -> &str {
        &self.inner.branch
    }

    pub fn task(&self) -> Option<&str> {
        self.inner.task.as_deref()
    }

    pub fn state(&self) -> RunState {
        *self.inner.state.lock().unwrap()
    }

    fn set_state(&self, s: RunState) {
        *self.inner.state.lock().unwrap() = s;
    }

    fn check_open(&self) -> Result<()> {
        let s = self.state();
        if s != RunState::Open {
            return Err(Error::new(
                ErrorKind::BranchClosed,
                format!("run {} is {s:?}", self.inner.branch).to_lowercase(),
            )
            .with_code("branch_closed"));
        }
        if self.inner.deadline.is_some_and(|d| Instant::now() > d) {
            return Err(
                Error::other(format!("run {} passed its time limit", self.inner.branch))
                    .with_code("run_deadline"),
            );
        }
        Ok(())
    }

    /// The envelope (spec): begin on the run, attribution, timeouts. The statement follows.
    async fn envelope(
        &self,
        conn: &mut PgConnection,
        reason: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let o = self.g.options();
        let seq = self.inner.seq.fetch_add(1, Ordering::SeqCst) + 1;
        sqlx::query("select gnomon.begin($1)")
            .bind(&self.inner.branch)
            .execute(&mut *conn)
            .await?;
        let meta = json!({ "run": self.inner.branch, "call": seq, "task": self.inner.task });
        sqlx::query("select gnomon.note($1, $2, $3::text::jsonb)")
            .bind(self.inner.actor.as_deref().unwrap_or("agent"))
            .bind(reason)
            .bind(meta.to_string())
            .execute(&mut *conn)
            .await?;
        let st = timeout.unwrap_or(o.statement_timeout);
        let st = ceil_ms(st).to_string();
        let lt = ceil_ms(o.lock_timeout).to_string();
        sqlx::query("select set_config('statement_timeout', $1, true), set_config('lock_timeout', $2, true)")
            .bind(st)
            .bind(lt)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    /// One statement on the run's branch. See [`Run::sql_opts`].
    pub async fn sql(&self, statement: &str) -> Result<SqlResult> {
        self.sql_opts(statement, PgArguments::default(), SqlOptions::default())
            .await
    }

    /// One statement with parameters (`$1`, `$2`, ...): `run.sql_with(s, gnomon::args![3_i64])`.
    pub async fn sql_with(&self, statement: &str, args: PgArguments) -> Result<SqlResult> {
        self.sql_opts(statement, args, SqlOptions::default()).await
    }

    /// One statement on the run's branch, in its own envelope. Rows past the cap come back with
    /// `truncated`. A gnomon refusal is a typed [`Error`] whose message is written for a model to
    /// read. A serialization failure (40001) is retried with backoff up to three times.
    pub async fn sql_opts(
        &self,
        statement: &str,
        args: PgArguments,
        opts: SqlOptions,
    ) -> Result<SqlResult> {
        self.check_open()?;
        self.g.ready().await?;
        one_statement(statement, "split it, or use transaction()")?;
        let pool = self.g.runs_or_err()?;
        let cap = opts.max_rows.unwrap_or(self.g.options().max_rows);
        let mut attempt = 0;
        loop {
            let t0 = Instant::now();
            let r: Result<SqlResult> = async {
                let mut tx = pool.begin().await?;
                self.envelope(&mut tx, opts.reason.as_deref(), opts.timeout)
                    .await?;
                let r = exec_one(&mut tx, statement, args.clone(), cap).await?;
                tx.commit().await?;
                Ok(r)
            }
            .await;
            self.g.report(
                Login::Runs,
                Some(&self.inner.branch),
                statement,
                t0,
                r.as_ref().map(|x| Some(x.row_count)),
            );
            match r {
                Err(e) if e.is_serialization() && attempt < RETRIES => {
                    backoff(attempt).await;
                    attempt += 1;
                }
                r => return r,
            }
        }
    }

    /// Several statements in one envelope, all or nothing. `f` is run again on a serialization
    /// failure, up to three times.
    ///
    /// ```no_run
    /// # async fn demo(run: gnomon::Run) -> gnomon::Result<()> {
    /// run.transaction(None, |tx| Box::pin(async move {
    ///     tx.sql("update acct set bal = bal - 5 where id = 1").await?;
    ///     tx.sql("update acct set bal = bal + 5 where id = 2").await?;
    ///     Ok(())
    /// })).await?;
    /// # Ok(()) }
    /// ```
    pub async fn transaction<T, F>(&self, reason: Option<&str>, mut f: F) -> Result<T>
    where
        F: for<'c> FnMut(&'c mut RunTx) -> BoxFuture<'c, Result<T>>,
    {
        self.check_open()?;
        self.g.ready().await?;
        let pool = self.g.runs_or_err()?;
        let mut attempt = 0;
        loop {
            let r: Result<T> = async {
                let mut tx = pool.begin().await?;
                self.envelope(&mut tx, reason, None).await?;
                let mut rtx = RunTx {
                    tx,
                    run: self.clone(),
                };
                let out = f(&mut rtx).await?;
                rtx.tx.commit().await?;
                Ok(out)
            }
            .await;
            match r {
                Err(e) if e.is_serialization() && attempt < RETRIES => {
                    backoff(attempt).await;
                    attempt += 1;
                }
                r => return r,
            }
        }
    }

    /// What the run changed, field by field (read by the app login).
    pub async fn diff(&self) -> Result<Vec<Change>> {
        self.g.diff(&self.inner.branch, None).await
    }

    /// Changes, checks, the conflicts a merge would hit, warnings: one round trip.
    pub async fn summary(&self) -> Result<Summary> {
        self.summary_opts(true).await
    }

    pub async fn summary_opts(&self, run_checks: bool) -> Result<Summary> {
        self.g
            .app_json_as(
                "select gnomon.run_summary($1, $2)::text",
                bind![&self.inner.branch, run_checks],
            )
            .await
    }

    pub async fn checks(&self) -> Result<Vec<Check>> {
        let rows = self
            .g
            .app_rows(
                "select check_name, passed, detail from gnomon.run_checks($1)",
                bind![&self.inner.branch],
            )
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Check {
                    check_name: r.try_get(0)?,
                    passed: r.try_get(1)?,
                    detail: r.try_get(2)?,
                })
            })
            .collect()
    }

    /// Accept the run into the real data and close its branch. Conflicts come back as an error of
    /// kind [`ErrorKind::MergeConflict`] carrying them: resolve, then merge again.
    pub async fn merge(&self) -> Result<MergeResult> {
        self.merge_opts(true).await
    }

    pub async fn merge_opts(&self, close: bool) -> Result<MergeResult> {
        use tracing::Instrument;
        let span = tracing::debug_span!(target: "gnomon", "gnomon.merge", run = %self.inner.branch);
        self.merge_inner(close).instrument(span).await
    }

    async fn merge_inner(&self, close: bool) -> Result<MergeResult> {
        let id: i64 = self
            .g
            .app_value(
                "select gnomon.merge($1, null, 'atomic', null, null, null, $2)::int8",
                bind![&self.inner.branch, close],
            )
            .await?;
        let m: MergeResult = self
            .g
            .app_json_as(
                "select jsonb_build_object('merge', m.id, 'status', m.status, 'stats', m.stats)::text from gnomon.merge m where m.id = $1",
                bind![id],
            )
            .await?;
        if m.status == "conflicts" {
            let conflicts: Vec<Value> = self
                .g
                .app_json_as(
                    "select coalesce(jsonb_agg(to_jsonb(c) order by c.id), '[]')::text from gnomon.conflict c where c.merge_id = $1",
                    bind![id],
                )
                .await?;
            return Err(Error::new(
                ErrorKind::MergeConflict {
                    merge_id: id,
                    conflicts: conflicts.clone(),
                },
                format!(
                    "merging run {} stopped on {} conflict(s)",
                    self.inner.branch,
                    conflicts.len()
                ),
            )
            .with_code("merge_conflict"));
        }
        // merged, whether or not the branch was closed: with_run must not discard it
        self.set_state(RunState::Merged);
        Ok(m)
    }

    /// Open a change request for a person to review ([`Request::wait`] waits for it).
    pub async fn submit(&self, title: Option<&str>) -> Result<Request> {
        self.submit_with(title.map(str::to_string), None).await
    }

    pub async fn submit_with(
        &self,
        title: Option<String>,
        body: Option<String>,
    ) -> Result<Request> {
        let title = title
            .or_else(|| self.inner.task.clone())
            .unwrap_or_else(|| format!("Run {}", self.inner.branch));
        let id: i64 = self
            .g
            .app_value(
                "select gnomon.open_change_request($1, $2, $3)::int8",
                bind![&self.inner.branch, title, body],
            )
            .await?;
        self.set_state(RunState::Submitted);
        Ok(Request::new(self.g.clone(), id))
    }

    /// Drop the run's branch and what it holds. Discarding twice is a no-op.
    pub async fn discard(&self) -> Result<()> {
        if self.state() == RunState::Discarded {
            return Ok(());
        }
        self.g
            .app_rows(
                "select gnomon.run_end($1, false)::text",
                bind![&self.inner.branch],
            )
            .await?;
        self.set_state(RunState::Discarded);
        Ok(())
    }

    /// A connection string pinned to the run, for a tool that wants one: a login of the run's own
    /// (`gnomon.run_login`), confined to the run's branch, dropped when the run ends. `base` gives
    /// the host, port and database (default: the builder's `url`). A second call rotates the
    /// password.
    pub async fn url(&self, base: Option<&str>) -> Result<String> {
        self.check_open()?;
        let b = base
            .map(str::to_string)
            .or_else(|| self.g.options().url.clone())
            .ok_or_else(|| {
                Error::other(
                    "Run::url needs a base connection string (the builder's url, or an argument)",
                )
                .with_code("run_url_base")
            })?;
        #[derive(Deserialize)]
        struct Login {
            role: String,
            password: String,
        }
        let login: Login = self
            .g
            .app_json_as(
                "select gnomon.run_login($1)::text",
                bind![&self.inner.branch],
            )
            .await?;
        put_login(&b, &login.role, &login.password)
    }

    /// A connection of the run's own, opened from [`Run::url`]. The base must be a URL
    /// (`postgres://...`): sqlx does not read libpq keyword strings.
    pub async fn connect(&self, base: Option<&str>) -> Result<PgConnection> {
        let b = base.or(self.g.options().url.as_deref()).unwrap_or("");
        if !b.is_empty() && !b.contains("://") {
            return Err(Error::other(
                "Run::connect needs a URL base (postgres://host/db); for a keyword string use Run::url with your own driver",
            ));
        }
        let url = self.url(base).await?;
        Ok(PgConnection::connect(&url).await?)
    }

    /// Remember the run's state under a name, to rewind to it later.
    pub async fn checkpoint(&self, name: &str) -> Result<String> {
        self.g
            .app_value(
                "select gnomon.checkpoint($1, $2)",
                bind![format!("{}:{name}", self.inner.branch), &self.inner.branch],
            )
            .await
    }

    /// Undo every change the run made since a checkpoint.
    pub async fn rewind(&self, name: &str) -> Result<Value> {
        self.g
            .app_json(
                "select gnomon.rewind($1, $2)::text",
                bind![&self.inner.branch, format!("{}:{name}", self.inner.branch)],
            )
            .await
    }

    /// Try statements without keeping them: the changes they would make, the checks, any error.
    pub async fn dry_run(&self, statements: &[String], checks: bool) -> Result<Value> {
        self.g
            .app_json(
                "select gnomon.dry_run($1, $2, $3)::text",
                bind![statements, &self.inner.branch, checks],
            )
            .await
    }

    /// The database as this run sees it (for describe tools).
    pub async fn describe(&self, schema: Option<&str>) -> Result<Value> {
        self.g.ready().await?;
        let pool = self.g.runs_or_err()?;
        let mut tx = pool.begin().await?;
        sqlx::query("select gnomon.begin($1)")
            .bind(&self.inner.branch)
            .execute(&mut *tx)
            .await?;
        let t: Option<String> = sqlx::query_scalar("select gnomon.describe($1)::text")
            .bind(schema)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        parse_json(t.as_deref())
    }
}

/// Put a role and password into a connection string: a URL's user info, a socket URL's query
/// parameters (libpq takes the user from there), or a keyword string's `user=`/`password=`.
pub(crate) fn put_login(base: &str, role: &str, password: &str) -> Result<String> {
    if base.contains("://") {
        let mut u = url::Url::parse(base).map_err(|e| Error::other(format!("base url: {e}")))?;
        // a user or password in the query wins over the user info, so drop the base's own
        let pairs: Vec<(String, String)> = u
            .query_pairs()
            .filter(|(k, _)| k != "user" && k != "password")
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if pairs.is_empty() {
            u.set_query(None);
        } else {
            u.query_pairs_mut().clear().extend_pairs(pairs);
        }
        if u.host_str().unwrap_or("").is_empty() {
            // a socket URL (postgresql:///db?host=/dir): no host part to carry a user
            u.query_pairs_mut()
                .append_pair("user", role)
                .append_pair("password", password);
        } else {
            u.set_username(role)
                .map_err(|_| Error::other("base url cannot carry a user"))?;
            u.set_password(Some(password))
                .map_err(|_| Error::other("base url cannot carry a password"))?;
        }
        return Ok(u.to_string());
    }
    Ok(format!("{base} user={role} password={password}"))
}

/// The open envelope of [`Run::transaction`]: each call is one statement on the run's branch.
pub struct RunTx {
    tx: Transaction<'static, Postgres>,
    run: Run,
}

impl RunTx {
    pub async fn sql(&mut self, statement: &str) -> Result<SqlResult> {
        self.sql_with(statement, PgArguments::default()).await
    }

    pub async fn sql_with(&mut self, statement: &str, args: PgArguments) -> Result<SqlResult> {
        one_statement(statement, "one statement per call")?;
        let t0 = Instant::now();
        // statements written in code are not capped (as in the other SDKs)
        let r = exec_one(&mut self.tx, statement, args, usize::MAX).await;
        self.run.g.report(
            Login::Runs,
            Some(self.run.branch()),
            statement,
            t0,
            r.as_ref().map(|x| Some(x.row_count)),
        );
        r
    }
}

#[cfg(test)]
mod tests {
    use super::{ceil_ms, put_login};
    use std::time::Duration;

    #[test]
    fn timeouts_never_zero() {
        assert_eq!(ceil_ms(Duration::ZERO), 1);
        assert_eq!(ceil_ms(Duration::from_micros(500)), 1);
        assert_eq!(ceil_ms(Duration::from_micros(1500)), 2);
        assert_eq!(ceil_ms(Duration::from_secs(30)), 30000);
    }

    #[test]
    fn login_into_urls() {
        assert_eq!(
            put_login("postgres://localhost:5432/shop", "r", "p").unwrap(),
            "postgres://r:p@localhost:5432/shop"
        );
        let s = put_login("postgresql:///shop?host=/run/pg", "r", "p").unwrap();
        assert!(
            s.contains("host=%2Frun%2Fpg") && s.contains("user=r") && s.contains("password=p"),
            "{s}"
        );
        assert_eq!(
            put_login("host=/tmp dbname=shop", "r", "p").unwrap(),
            "host=/tmp dbname=shop user=r password=p"
        );
    }
}
