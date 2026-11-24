//! Runs: one agent task on a branch of its own, behind the envelope (spec: The envelope).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{
    AssertSqlSafe, Connection, Either, Executor, PgConnection, PgPool, Postgres, Row, ValueRef,
};

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
    /// Bytes of row data kept before `truncated` (default [`Options::max_bytes`](crate::Options)).
    pub max_bytes: Option<usize>,
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
    /// The change request `submit` opened. Read-held for the length of every call on the run's
    /// branch, write-held by `submit`, `merge` and `discard`: they wait for calls in flight, and
    /// a call that starts after them finds the run closed.
    gate: tokio::sync::RwLock<Option<i64>>,
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

/// Whether a statement starts, ends or leaves a transaction (`ROLLBACK TO` a savepoint is fine).
/// Inside an envelope it would end the run's transaction and run what follows outside it. The
/// first keyword decides a statement's kind, so this reads up to three words the way
/// PostgreSQL's lexer does: its whitespace (including `\v` and `\f`), `--` comments ending at
/// `\n` or `\r`, nested `/* */` comments, and empty statements (`;`) before the first word.
fn transaction_control(statement: &str) -> bool {
    let b = statement.as_bytes();
    let mut i = 0;
    let mut words: Vec<String> = Vec::new();
    while i < b.len() && words.len() < 3 {
        match b[i] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => i += 1,
            b';' if words.is_empty() => i += 1,
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' && b[i] != b'\r' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0;
                while i < b.len() {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let s = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                words.push(statement[s..i].to_ascii_lowercase());
            }
            _ => break,
        }
    }
    let w = |n: usize| words.get(n).map(String::as_str);
    match w(0) {
        Some("begin" | "start" | "commit" | "end" | "abort") => true,
        Some("rollback") => {
            let next = match w(1) {
                Some("work" | "transaction") => w(2),
                x => x,
            };
            next != Some("to")
        }
        Some("prepare") => w(1) == Some("transaction"),
        _ => false,
    }
}

fn refuse_transaction_control(statement: &str) -> Result<()> {
    if transaction_control(statement) {
        return Err(Error::other(
            "transaction control is not allowed inside a run's transaction: it is one transaction already",
        )
        .with_code("transaction_control")
        .with_sqlstate("25001"));
    }
    Ok(())
}

/// What `DISCARD ALL` does, less `DISCARD PLANS` and an unconditional `DISCARD TEMP`. Both made
/// every call about twice as slow: gnomon keeps a temporary table of its own per session
/// (`gnomon_run_bind`, owned by the extension's owner, out of the runs login's reach) and
/// rebuilds it, and its functions replan; a plan carries no state between runs. The last
/// statement says whether the temporary schema holds anything else (a run's own temporary
/// table, view, function, type, ...), which [`RunsTx::begin`] then discards. Every catalog is
/// named `pg_catalog.*`: a temporary view named `pg_class` would otherwise be read instead.
const RESET_SESSION: &str = "close all; set session authorization default; reset all; \
deallocate all; unlisten *; select pg_catalog.pg_advisory_unlock_all(); discard sequences; \
select exists (select from ( \
  select relnamespace, relowner from pg_catalog.pg_class union all \
  select typnamespace, typowner from pg_catalog.pg_type union all \
  select pronamespace, proowner from pg_catalog.pg_proc union all \
  select oprnamespace, oprowner from pg_catalog.pg_operator union all \
  select collnamespace, collowner from pg_catalog.pg_collation union all \
  select connamespace, conowner from pg_catalog.pg_conversion union all \
  select cfgnamespace, cfgowner from pg_catalog.pg_ts_config union all \
  select dictnamespace, dictowner from pg_catalog.pg_ts_dict union all \
  select stxnamespace, stxowner from pg_catalog.pg_statistic_ext \
) o(ns, owner) \
where ns = pg_catalog.pg_my_temp_schema() \
  and owner is distinct from (select extowner from pg_catalog.pg_extension where extname = 'gnomon'))";

/// A transaction on the runs login. The connection is reset first ([`RESET_SESSION`]), so
/// nothing a statement of one run left in the session (temporary objects, settings, cursors,
/// prepared statements, advisory locks) reaches the next run that gets the connection. Role
/// defaults a run sets with `alter role current_user set` are the one exception: they live in
/// the catalog, not the session, and only gnomon's confinement could refuse them. Dropped
/// without [`commit`](Self::commit) or [`rollback`](Self::rollback) (an error on the way, a
/// cancelled future), the connection goes to a task that waits out the statement in flight and
/// rolls back before it returns to the pool, as sqlx's own transactions do; so the server never
/// holds more runs-login backends than the pool allows.
pub(crate) struct RunsTx {
    conn: Option<PoolConnection<Postgres>>,
    done: bool,
    xid: Option<String>,
    /// statement_timeout and lock_timeout (ms), put back after every statement
    timeouts: Option<(String, String)>,
}

impl RunsTx {
    pub(crate) async fn begin(pool: &PgPool) -> Result<RunsTx> {
        let mut conn = pool.acquire().await?;
        let reset = async {
            // the reset deallocates every prepared statement: forget sqlx's cache of them first,
            // so a statement a run prepared under one of sqlx's names is never executed as sqlx's
            conn.clear_cached_statements().await?;
            let rows = sqlx::raw_sql(RESET_SESSION).fetch_all(&mut *conn).await?;
            let dirty = match rows.last() {
                Some(r) => r.try_get::<bool, _>(0)?,
                None => true,
            };
            if dirty {
                sqlx::raw_sql("discard temp").execute(&mut *conn).await?;
            }
            sqlx::raw_sql("begin").execute(&mut *conn).await?;
            Ok::<_, Error>(())
        }
        .await;
        if let Err(e) = reset {
            // a session that cannot be reset is never handed to another run
            conn.close_on_drop();
            return Err(e);
        }
        Ok(RunsTx {
            conn: Some(conn),
            done: false,
            xid: None,
            timeouts: None,
        })
    }

    /// The transaction's id, setting the run's timeouts again (local to the transaction).
    async fn xact_id(&mut self) -> Result<String> {
        let (st, lt) = self
            .timeouts
            .clone()
            .ok_or_else(|| Error::other("RunsTx::mark sets the timeouts first"))?;
        Ok(sqlx::query_scalar(
            "select pg_catalog.pg_current_xact_id()::text, \
                    pg_catalog.set_config('statement_timeout', $1, true), \
                    pg_catalog.set_config('lock_timeout', $2, true)",
        )
        .bind(st)
        .bind(lt)
        .persistent(false)
        .fetch_one(&mut **self)
        .await?)
    }

    /// Remember the transaction and the run's timeouts, for [`check`](Self::check).
    async fn mark(&mut self, statement_timeout: Duration, lock_timeout: Duration) -> Result<()> {
        self.timeouts = Some((
            ceil_ms(statement_timeout).to_string(),
            ceil_ms(lock_timeout).to_string(),
        ));
        self.xid = Some(self.xact_id().await?);
        Ok(())
    }

    /// Still the transaction [`mark`](Self::mark) saw, and not aborted. Puts the run's timeouts
    /// back, so a statement that changed them (`set statement_timeout = 0`) affects only itself.
    async fn check(&mut self) -> Result<()> {
        let now = self.xact_id().await?;
        if self.xid.as_deref() != Some(now.as_str()) {
            return Err(Error::other(
                "a statement ended the run's transaction; nothing after it was kept",
            )
            .with_code("transaction_control")
            .with_sqlstate("25001"));
        }
        Ok(())
    }

    async fn commit(mut self) -> Result<()> {
        sqlx::raw_sql("commit").execute(&mut *self).await?;
        self.done = true;
        Ok(())
    }

    async fn rollback(mut self) -> Result<()> {
        sqlx::raw_sql("rollback").execute(&mut *self).await?;
        self.done = true;
        Ok(())
    }
}

impl Drop for RunsTx {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    if sqlx::raw_sql("rollback").execute(&mut *conn).await.is_err() {
                        conn.close_on_drop();
                    }
                });
            }
            Err(_) => conn.close_on_drop(),
        }
    }
}

impl std::ops::Deref for RunsTx {
    type Target = PgConnection;
    fn deref(&self) -> &PgConnection {
        self.conn
            .as_ref()
            .expect("an open RunsTx has its connection")
    }
}

impl std::ops::DerefMut for RunsTx {
    fn deref_mut(&mut self) -> &mut PgConnection {
        self.conn
            .as_mut()
            .expect("an open RunsTx has its connection")
    }
}

/// Bytes of a row's values as received.
fn row_bytes(row: &PgRow) -> usize {
    (0..row.len())
        .filter_map(|i| row.try_get_raw(i).ok())
        .filter(|v| !v.is_null())
        .map(|v| v.as_bytes().map_or(0, <[u8]>::len))
        .sum()
}

/// Send one statement by the extended protocol and keep its rows and its row count. Rows past
/// `cap`, or past `max_bytes` of row data, are read and dropped.
async fn exec_one(
    conn: &mut PgConnection,
    statement: &str,
    args: PgArguments,
    cap: usize,
    max_bytes: usize,
) -> Result<SqlResult> {
    let q = sqlx::query_with(AssertSqlSafe(statement.to_string()), args).persistent(false);
    let mut stream = conn.fetch_many(q);
    let mut rows = Vec::new();
    let mut row_count = 0u64;
    let mut seen = 0usize;
    let mut bytes = 0usize;
    let mut full = false;
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
                if !full && rows.len() < cap {
                    bytes = bytes.saturating_add(row_bytes(&row));
                    if bytes > max_bytes {
                        full = true;
                    } else {
                        rows.push(row);
                    }
                }
            }
        }
    }
    let truncated = seen > rows.len();
    Ok(SqlResult {
        rows,
        row_count,
        fields: fields.unwrap_or_default(),
        truncated,
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
                gate: tokio::sync::RwLock::new(None),
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

    /// Enter a call on the run's branch: held until the call ends (see `RunInner::gate`).
    async fn open_call(&self) -> Result<tokio::sync::RwLockReadGuard<'_, Option<i64>>> {
        let gate = self.inner.gate.read().await;
        self.check_open()?;
        Ok(gate)
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
        let _open = self.open_call().await?;
        self.g.ready().await?;
        one_statement(statement, "split it, or use transaction()")?;
        refuse_transaction_control(statement)?;
        let pool = self.g.runs_or_err()?;
        let cap = opts.max_rows.unwrap_or(self.g.options().max_rows);
        let max_bytes = opts.max_bytes.unwrap_or(self.g.options().max_bytes);
        let mut attempt = 0;
        loop {
            let t0 = Instant::now();
            let r: Result<SqlResult> = async {
                let mut tx = RunsTx::begin(pool).await?;
                let r = async {
                    self.envelope(&mut tx, opts.reason.as_deref(), opts.timeout)
                        .await?;
                    exec_one(&mut tx, statement, args.clone(), cap, max_bytes).await
                }
                .await;
                match r {
                    Ok(r) => {
                        tx.commit().await?;
                        Ok(r)
                    }
                    Err(e) => {
                        let _ = tx.rollback().await;
                        Err(e)
                    }
                }
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
    /// failure, up to three times. Inside `f`, use `tx`, never the run's own methods:
    /// [`submit`](Self::submit), [`merge`](Self::merge) or [`discard`](Self::discard) called from
    /// `f` waits for this very call to end, so it never returns; and any other method of the run
    /// blocks while a `submit` from elsewhere waits on this call.
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
        let _open = self.open_call().await?;
        self.g.ready().await?;
        let pool = self.g.runs_or_err()?;
        let mut attempt = 0;
        loop {
            let r: Result<T> = async {
                let mut tx = RunsTx::begin(pool).await?;
                self.envelope(&mut tx, reason, None).await?;
                tx.mark(
                    self.g.options().statement_timeout,
                    self.g.options().lock_timeout,
                )
                .await?;
                let mut rtx = RunTx {
                    tx,
                    run: self.clone(),
                };
                let out = async {
                    let out = f(&mut rtx).await?;
                    // a statement the closure's error handling swallowed left it aborted: commit
                    // would roll back and report success
                    rtx.tx.check().await?;
                    Ok(out)
                }
                .await;
                match out {
                    Ok(out) => {
                        rtx.tx.commit().await?;
                        Ok(out)
                    }
                    Err(e) => {
                        let _ = rtx.tx.rollback().await;
                        Err(e)
                    }
                }
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
        let _gate = self.inner.gate.write().await;
        let m = merge_branch(&self.g, &self.inner.branch, close).await?;
        // merged, whether or not the branch was closed: with_run must not discard it
        self.set_state(RunState::Merged);
        Ok(m)
    }

    /// Open a change request for a person to review ([`Request::wait`] waits for it). Submitting
    /// again returns the same request, title and body unchanged.
    pub async fn submit(&self, title: Option<&str>) -> Result<Request> {
        self.submit_with(title.map(str::to_string), None).await
    }

    pub async fn submit_with(
        &self,
        title: Option<String>,
        body: Option<String>,
    ) -> Result<Request> {
        let mut request = self.inner.gate.write().await;
        if let Some(id) = *request {
            return Ok(Request::new(self.g.clone(), id));
        }
        self.check_open()?;
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
        *request = Some(id);
        self.set_state(RunState::Submitted);
        Ok(Request::new(self.g.clone(), id))
    }

    /// Drop the run's branch and what it holds. Discarding twice is a no-op.
    pub async fn discard(&self) -> Result<()> {
        let _gate = self.inner.gate.write().await;
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
    ///
    /// On the runs login, in the run's envelope, like the run's statements.
    pub async fn checkpoint(&self, name: &str) -> Result<String> {
        self.check_open()?;
        let tag = format!("{}:{name}", self.inner.branch);
        let branch = self.inner.branch.clone();
        self.transaction(Some("checkpoint"), |tx| {
            let args = (tag.clone(), branch.clone());
            Box::pin(async move {
                let r = tx
                    .sql_with("select gnomon.checkpoint($1, $2)", bind![args.0, args.1])
                    .await?;
                first_value::<String>(&r)
            })
        })
        .await
    }

    /// Undo every change the run made since a checkpoint.
    ///
    /// On the runs login, in the run's envelope, like the run's statements.
    pub async fn rewind(&self, name: &str) -> Result<Value> {
        self.check_open()?;
        let tag = format!("{}:{name}", self.inner.branch);
        let branch = self.inner.branch.clone();
        self.transaction(Some("rewind"), |tx| {
            let args = (branch.clone(), tag.clone());
            Box::pin(async move {
                let r = tx
                    .sql_with("select gnomon.rewind($1, $2)::text", bind![args.0, args.1])
                    .await?;
                parse_json(first_value::<Option<String>>(&r)?.as_deref())
            })
        })
        .await
    }

    /// Try statements without keeping them: the changes they would make, the checks, any error.
    ///
    /// Runs on the runs login, in the run's envelope, and always rolls back. (`gnomon.dry_run`
    /// itself is refused to a confined login, and on the app login it would run a model's
    /// statements with the app's privileges.) The changes are those the statements make on top
    /// of the run's own, as `gnomon.dry_run` reports them.
    pub async fn dry_run(&self, statements: &[String], checks: bool) -> Result<Value> {
        let _open = self.open_call().await?;
        self.g.ready().await?;
        let pool = self.g.runs_or_err()?;
        let mut tx = RunsTx::begin(pool).await?;
        let before = async {
            self.envelope(&mut tx, Some("dry run"), None).await?;
            tx.mark(
                self.g.options().statement_timeout,
                self.g.options().lock_timeout,
            )
            .await?;
            branch_diff(&mut tx, &self.inner.branch).await
        }
        .await;
        let before = match before {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
            }
        };
        let mut failed: Option<(usize, Error)> = None;
        for (i, st) in statements.iter().enumerate() {
            let t0 = Instant::now();
            let r = async {
                one_statement(st, "one statement per element")?;
                refuse_transaction_control(st)?;
                // through plpgsql's EXECUTE, as gnomon.dry_run does: there a COMMIT or ROLLBACK
                // cannot end the transaction, whatever the text looks like to a scanner
                sqlx::query("select pg_catalog.set_config('gnomon_rs.statement', $1, true)")
                    .bind(st)
                    .persistent(false)
                    .execute(&mut *tx)
                    .await?;
                let r = exec_one(&mut tx, DRY_STATEMENT, PgArguments::default(), 0, 0).await?;
                tx.check().await?;
                Ok::<_, Error>(r)
            }
            .await;
            self.g.report(
                Login::Runs,
                Some(&self.inner.branch),
                st,
                t0,
                r.as_ref().map(|x| Some(x.row_count)),
            );
            if let Err(e) = r {
                failed = Some((i + 1, e));
                break;
            }
        }
        if let Some((i, e)) = failed {
            let _ = tx.rollback().await;
            let explanation = self.g.explain_error(&e).await.ok().flatten();
            return Ok(json!({
                "ok": false,
                "error": e.message(),
                "sqlstate": e.sqlstate(),
                "statement": i,
                "explanation": explanation,
            }));
        }
        let after = async {
            let after = branch_diff(&mut tx, &self.inner.branch).await?;
            let checks = if checks {
                let rows =
                    sqlx::query("select check_name, passed, detail from gnomon.run_checks($1)")
                        .bind(&self.inner.branch)
                        .fetch_all(&mut *tx)
                        .await?;
                let mut out = Vec::new();
                for r in rows {
                    out.push(json!({
                        "check": r.try_get::<String, _>(0)?,
                        "passed": r.try_get::<bool, _>(1)?,
                        "detail": r.try_get::<Option<String>, _>(2)?,
                    }));
                }
                Value::Array(out)
            } else {
                Value::Null
            };
            Ok::<_, Error>((after, checks))
        }
        .await;
        let _ = tx.rollback().await;
        let (after, checks) = after?;
        let mut diff = crate::dry::delta(&before, &after);
        // tables in the order gnomon.diff lists them (tracking order), then keys ascending
        crate::dry::sort(
            &mut diff,
            after.iter().chain(&before).map(|c| c.table.as_str()),
        );
        self.g.name_tables(&mut diff).await?;
        Ok(json!({
            "ok": true,
            "statements": statements.len(),
            "summary": crate::dry::summary(&diff),
            "diff": diff,
            "checks": checks,
            "note": "Nothing was kept. Sequence values drawn on the way are not returned.",
        }))
    }

    /// The database as this run sees it (for describe tools).
    pub async fn describe(&self, schema: Option<&str>) -> Result<Value> {
        let _open = self.open_call().await?;
        self.g.ready().await?;
        let pool = self.g.runs_or_err()?;
        let mut tx = RunsTx::begin(pool).await?;
        let t: Result<Option<String>> = async {
            sqlx::query("select gnomon.begin($1)")
                .bind(&self.inner.branch)
                .execute(&mut *tx)
                .await?;
            sqlx::query("select set_config('statement_timeout', $1, true)")
                .bind(ceil_ms(self.g.options().statement_timeout).to_string())
                .execute(&mut *tx)
                .await?;
            Ok(sqlx::query_scalar("select gnomon.describe($1)::text")
                .bind(schema)
                .fetch_one(&mut *tx)
                .await?)
        }
        .await;
        let _ = tx.rollback().await;
        parse_json(t?.as_deref())
    }
}

/// Merge a branch into main (atomic). Anything but an applied merge is an error: conflicts as
/// [`ErrorKind::MergeConflict`] carrying them.
pub(crate) async fn merge_branch(g: &Gnomon, branch: &str, close: bool) -> Result<MergeResult> {
    let id: i64 = g
        .app_value(
            "select gnomon.merge($1, null, 'atomic', null, null, null, $2)::int8",
            bind![branch, close],
        )
        .await?;
    let m: MergeResult = g
        .app_json_as(
            "select jsonb_build_object('merge', m.id, 'status', m.status, 'stats', m.stats)::text from gnomon.merge m where m.id = $1",
            bind![id],
        )
        .await?;
    if m.status == "conflicts" {
        let conflicts: Vec<Value> = g
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
                "merging {branch} stopped on {} conflict(s)",
                conflicts.len()
            ),
        )
        .with_code("merge_conflict"));
    }
    if m.status != "applied" {
        return Err(Error::other(format!(
            "merge {id} of {branch} ended {}, not applied",
            m.status
        ))
        .with_code("merge_not_applied"));
    }
    Ok(m)
}

/// The first column of the first row.
fn first_value<T>(r: &SqlResult) -> Result<T>
where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    let row = r
        .rows
        .first()
        .ok_or_else(|| Error::other("expected a row, got none"))?;
    Ok(row.try_get(0)?)
}

/// Runs the statement a dry run put in `gnomon_rs.statement` through plpgsql's `EXECUTE`. The
/// statement is a bound parameter, never part of this text, so no quoting can be escaped.
const DRY_STATEMENT: &str =
    "do $gnomon$ begin execute pg_catalog.current_setting('gnomon_rs.statement'); end $gnomon$";

/// The run's diff as seen inside its own envelope (uncommitted statements included). Tables
/// come back as oids: the runs login's search path would qualify their names differently from
/// the app's (see [`Gnomon::name_tables`]).
async fn branch_diff(conn: &mut PgConnection, branch: &str) -> Result<Vec<Change>> {
    let rows = sqlx::query(
        "select tbl::oid::text, key::text, kind, col, old::text, new::text from gnomon.diff($1, null)",
    )
    .bind(branch)
    .fetch_all(&mut *conn)
    .await?;
    rows.iter().map(crate::change_from_row).collect()
}

/// Put a role and password into a connection string: a URL's user info, a socket URL's query
/// parameters (libpq takes the user from there), or a keyword string's `user=`/`password=`.
pub(crate) fn put_login(base: &str, role: &str, password: &str) -> Result<String> {
    if base.contains("://") {
        let mut u = url::Url::parse(base).map_err(|e| Error::other(format!("base url: {e}")))?;
        // a user or password in the query wins over the user info, so drop the base's own. The
        // rest is kept byte for byte: libpq percent-decodes a query but reads `+` as itself, so
        // re-encoding it as a form (`%20` -> `+`) would change what libpq sees
        let mut query: Vec<String> = u
            .query()
            .unwrap_or("")
            .split('&')
            .filter(|seg| {
                let key = url::form_urlencoded::parse(seg.as_bytes())
                    .next()
                    .map(|(k, _)| k.into_owned());
                key.is_some_and(|k| k != "user" && k != "password")
            })
            .map(|seg| seg.replace('+', "%2B"))
            .collect();
        let socket = u.host_str().unwrap_or("").is_empty();
        if socket {
            // a socket URL (postgresql:///db?host=/dir): no host part to carry a user
            query.push(format!("user={}", percent(role)));
            query.push(format!("password={}", percent(password)));
        }
        u.set_query((!query.is_empty()).then(|| query.join("&")).as_deref());
        if !socket {
            u.set_username(role)
                .map_err(|_| Error::other("base url cannot carry a user"))?;
            u.set_password(Some(password))
                .map_err(|_| Error::other("base url cannot carry a password"))?;
        }
        return Ok(u.to_string());
    }
    Ok(format!(
        "{base} user={} password={}",
        keyword_value(role),
        keyword_value(password)
    ))
}

/// Percent-encode everything but RFC 3986's unreserved characters, so libpq and sqlx (which
/// form-decodes) read the same value.
fn percent(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A libpq keyword value, single-quoted with `\` and `'` escaped.
fn keyword_value(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The open envelope of [`Run::transaction`]: each call is one statement on the run's branch.
pub struct RunTx {
    tx: RunsTx,
    run: Run,
}

impl RunTx {
    pub async fn sql(&mut self, statement: &str) -> Result<SqlResult> {
        self.sql_with(statement, PgArguments::default()).await
    }

    pub async fn sql_with(&mut self, statement: &str, args: PgArguments) -> Result<SqlResult> {
        one_statement(statement, "one statement per call")?;
        refuse_transaction_control(statement)?;
        let t0 = Instant::now();
        // statements written in code are not capped (as in the other SDKs)
        let r = async {
            let r = exec_one(&mut self.tx, statement, args, usize::MAX, usize::MAX).await?;
            self.tx.check().await?;
            Ok(r)
        }
        .await;
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
    fn transaction_control_as_postgres_reads_it() {
        use super::transaction_control as tc;
        for yes in [
            "commit",
            "COMMIT AND CHAIN",
            "end",
            "abort",
            "begin",
            "start transaction",
            "rollback",
            "rollback work",
            "prepare transaction 'x'",
            "\x0bcommit",
            "\x0ccommit",
            "-- a\rcommit",
            "-- a\ncommit",
            "/* a /* b */ c */ commit",
            ";commit",
            " ; ; commit",
            "commit prepared 'x'",
            "rollback prepared 'x'",
        ] {
            assert!(tc(yes), "{yes:?}");
        }
        for no in [
            "rollback to s",
            "rollback work to s",
            "rollback   to savepoint s",
            "savepoint s",
            "release s",
            "select 1",
            "\"commit\"",
            "prepare p as select 1",
            "update t set commit = 1",
            "-- commit\nselect 1",
        ] {
            assert!(!tc(no), "{no:?}");
        }
    }

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
        assert_eq!(
            put_login("postgresql:///shop?host=/run/pg&user=x", "r", "p+q/").unwrap(),
            "postgresql:///shop?host=/run/pg&user=r&password=p%2Bq%2F"
        );
        // the base's query kept byte for byte, its own user dropped however it is spelled
        assert_eq!(
            put_login(
                "postgres://u:pw@h/db?options=-c%20x%3Dy&%75ser=postgres&password=s&application_name=a+b",
                "r",
                "p"
            )
            .unwrap(),
            "postgres://r:p@h/db?options=-c%20x%3Dy&application_name=a%2Bb"
        );
        assert_eq!(
            put_login("host=/tmp dbname=shop", "r", "p").unwrap(),
            "host=/tmp dbname=shop user='r' password='p'"
        );
        assert_eq!(
            put_login("host=/tmp", "r", "a b'c\\").unwrap(),
            r"host=/tmp user='r' password='a b\'c\\'"
        );
    }
}
