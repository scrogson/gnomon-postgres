# gnomon-postgres

A Rust client for [Gnomon](https://gnomon.bitsinfra.com/): git for Postgres data, built for agents.
Gnomon is a PostgreSQL extension that gives a database branches, history, merges and undo. This
crate is a port of the official TypeScript SDK (`@bitsinfra/gnomon`) to Rust on
[sqlx](https://github.com/launchbadge/sqlx) 0.9. It implements the same
[wire specification](spec/SPEC.md) and passes the same conformance scenarios.

The package is `gnomon-postgres` (the name `gnomon` is taken on crates.io); the library is `gnomon`.

```toml
[dependencies]
gnomon-postgres = { git = "https://github.com/scrogson/gnomon-postgres" }
```

Requires the extension at SQL API 2 (gnomon 1.0.x) on PostgreSQL 15–18, Rust 1.94+, and tokio.

## A run

Give each agent task a branch of its own. Everything it writes lands on the branch; you read the
diff, then merge it, send it for review, or throw it away.

```rust
use gnomon::{Gnomon, StartRun};
use sqlx::PgPool;

let app = PgPool::connect("postgres://shop_app@localhost/shop").await?;   // member of gnomon_runner
let runs = PgPool::connect("postgres://shop_runs@localhost/shop").await?; // the confined runs login
let gnomon = Gnomon::builder(app).runs(runs).build();

gnomon.with_run(StartRun::new().task("Refund order 3").actor("support-bot"), |run| async move {
    run.sql("update orders set status = 'refunded' where id = 3").await?;
    let summary = run.summary().await?;
    if summary.checks.passed && summary.changes.rows <= 20 {
        run.merge().await?;
    } else {
        run.submit(Some("Refund order 3")).await?;
    }
    Ok(())
}).await?;
```

`with_run` discards the run unless the closure merged or submitted it, even if the closure returns
an error (a panic leaves it open). Rust has no async drop, so a `Run` you hold yourself stays open
until you call `discard()` or its TTL passes.

### Two logins

- **app**: your application's login, a member of `gnomon_runner`, plus `gnomon_merger` if your
  code merges into main. It starts runs, reads diffs and merges.
- **runs**: one confined login that every run shares. Name it once:

  ```sql
  select gnomon.set('runs_role', 'app_runs');
  select gnomon.confine('app_runs', array[]::text[], false);
  ```

Each run statement goes out on the runs login in an envelope: `gnomon.begin(branch)`,
`gnomon.note(actor, reason, meta)`, statement and lock timeouts, then exactly one statement over
the extended protocol. The database refuses a second statement, writes outside the run's branch,
reads of other runs' branches and of the real tables, and merges.

Every runs-login transaction starts on a reset connection (what `DISCARD ALL` does, with
`DISCARD TEMP` only when a run left temporary objects, and without dropping cached plans), so
temporary tables and views, settings, cursors, prepared statements and advisory locks one run
leaves in a session never reach the next run that gets the connection. Settings and `SET ROLE`
that your own `after_connect` hook puts on the runs pool are reset too: give settings as
connection options instead, and connect the runs pool as the runs login itself. A call cancelled
mid-statement (a dropped future, a timeout) rolls back on a background task before its
connection returns to the pool, so the server never holds more runs-login backends than the
pool allows.

### What a run does

| | |
|---|---|
| `run.sql(s)`, `run.sql_with(s, args![..])`, `run.sql_opts(s, args, SqlOptions { reason, timeout, max_rows, max_bytes })` | One statement on the branch. Rows are capped at 1000 rows and 16 MiB of row data (`truncated`); rows past either cap are read and dropped, so a single row larger than the byte cap is still read into memory before it is dropped. A 40001 is retried 3 times with backoff. Transaction control (`COMMIT`, `PREPARE TRANSACTION`, ...) is refused. |
| `run.transaction(reason, \|tx\| Box::pin(async move { .. }))` | Several statements in one envelope, all or nothing, rows not capped. Retried by running the closure again. `BEGIN`, `COMMIT`, `ROLLBACK` (but not `ROLLBACK TO`) and the like are refused, and the transaction is checked to be the same one after every statement. |
| `run.diff()`, `run.summary()`, `run.checks()` | What changed, per field; changes, checks, conflicts and warnings in one call. |
| `run.merge()`, `run.submit(title)`, `run.discard()` | Into the real data, to a person, or away. A conflict is `ErrorKind::MergeConflict { merge_id, conflicts }`. Submitting twice (two `done` calls, even at once) returns the same request. `submit`, `merge` and `discard` wait for the run's calls in flight. |
| `run.checkpoint(name)`, `run.rewind(name)`, `run.dry_run(&stmts, checks)` | Savepoints across calls, and trying statements without keeping them. All three run on the runs login in the run's envelope; `dry_run` always rolls back (see below). |
| `run.url(base)`, `run.connect(base)` | A connection string (URL or libpq keywords), or an open `PgConnection` (URL base only), for a login of the run's own, confined to its branch. |
| `run.describe(schema)` | The database as the run sees it. |

`SqlResult.rows` are sqlx `PgRow`s. `json_rows()` (or `gnomon::row_to_json`) turns them into JSON
objects: booleans, integers, floats, numeric, text types, enums, json, uuid, dates, times,
intervals, bytea, and arrays of the common scalars. Any other type (inet, money, ranges, ...) is
an error telling you to cast it with `::text`; `row_to_json_lossy` puts a placeholder string there
instead. The `sql` tool uses the lossy form, so a statement that committed is never reported to a
model as failed.

### `dry_run` differs from the spec

The spec maps `run.dryRun` to `gnomon.dry_run(statements, branch)`. That function refuses a
confined login, so an SDK has to call it on the app login, where a model's statements would run
with the application's privileges and no statement timeout. This crate runs the statements on the
runs login instead, inside the run's envelope, and rolls back. Each statement is sent as a bound
parameter (never spliced into SQL) and runs through plpgsql's `EXECUTE` in a fixed `DO` block, as
`gnomon.dry_run` does, so a `COMMIT` or `ROLLBACK` among them cannot end the transaction. The
run's statement and lock timeouts are applied again after every statement, so a `SET` among them
cannot lift them. The result has the same shape
(`ok`, `statements`, `summary`, `diff`, `checks`, or `error`/`sqlstate`/`statement`/`explanation`);
`diff` is worked out from the run's diff before and after, so it holds only what the statements
change, relative to the run's branch.

## Review

What your product's own Approve, Reject and Undo buttons call:

```rust
for req in gnomon.requests().open().await? {
    println!("{}", req.status().await?);
}
let req = gnomon.requests().get(id);
req.approve(Some("looks right")).await?;
req.merge().await?;
req.undo().await?;
req.wait(Duration::from_secs(300)).await?;          // gnomon.wait_for_review
req.comment("why zero?", CommentAt { table: Some("orders".into()), ..Default::default() }).await?;
```

`Request::merge` fails like `Run::merge`: a conflict is `ErrorKind::MergeConflict`, and any
merge that did not apply is an error.

## Core

`gnomon.branches()` has `create`, `scratch`, `drop`, `rebase`, `list` and `connection_options`. On
`Gnomon` itself: `diff(a, b)`, `history`, `blame`, `revert`, `status`, `doctor`, `describe`,
`explain_error` (`None` when gnomon has nothing to say) and `app_sql`.

## Tools for models

The seven tools every gnomon SDK gives an agent (`sql`, `describe`, `diff`, `dry_run`,
`checkpoint`, `rewind`, `done`) are bound to one run. Their descriptions come from the spec's
`tools.json`, embedded unchanged:

```rust
use gnomon::tools::{anthropic_tools, run_tool};

let tools = anthropic_tools();                  // or openai_tools(), tool_specs()
// ... the model asks for a tool ...
let out = run_tool(&run, &name, &input).await;  // text for the model; errors are text too
```

There are no per-framework adapters. Wire these into your own agent loop.

## Testing

`gnomon::testing` has `with_scratch` (a scratch branch per test) and `gnomon_diff`, which renders a
branch's diff as a stable snapshot (placeholders for keys, foreign keys followed) for `insta` or a
golden file. It also has `assert_no_changes` and `assert_changed`.

## Errors

`gnomon::Error` has `kind()`, `code()` (the stable `[gnomon:<slug>]`, read from the hint), `sqlstate()`, `hint()` and
the driver's error as `source()`. The kinds are the spec's classes: `Serialization`, `Permission`,
`BranchClosed`, `StatementTimeout`, `UnsupportedOnBranch`, `NotRouted`, `MergeConflict`,
`ChecksFailed`, `OverQuota`, `IncompatibleVersion`, `Other`. The client checks
`gnomon.api_version()` on first use.

## Exact numbers

Values follow the spec's rule. An integer that fits `i64`/`u64` is a JSON number, and so is a
decimal an `f64` prints back unchanged (`0.1`, `-2.5`). Anything else stays a JSON string with the
database's exact text: a `numeric(12,2)` of `1.10` is `"1.10"`, and an integer past 2^64 is a
string. Numbers inside `json`/`jsonb` values follow the same rule. A `float4`/`float8` column
is always a JSON number (`NaN` and `±Infinity` are strings), since a float is exact as itself.
The crate does not turn on serde_json's `arbitrary_precision`, which would change number
handling (`f64` under `#[serde(flatten)]` or `untagged`) for every crate in your build.

### Known differences from PostgreSQL's text

`infinity` dates and timestamps are an error in `row_to_json` (a placeholder in the lossy form),
BC dates print as ISO years (`44 BC` is `-0043`), and fractional seconds of times and timestamps
are padded to 3 or 6 digits (`12:00:00.500`, where PostgreSQL prints `12:00:00.5`). Cast with
`::text` when you need the server's exact text. JSON nested more than 400 levels deep is an
error rather than a risk of overflowing the stack. In a diff it does not fail the diff: the part
past 400 levels becomes a string saying so, with a hash of what it replaces (so a change past
the cap still shows as a change), and the rest of the row is shown.

## Security notes

Isolation between runs rests on the database and on this client sending `gnomon.begin` first and
one statement after it. Some limits belong to gnomon itself (1.0.1), not to this crate:

- A login from `Run::url` / `Run::connect` is a member of the shared runs role. It can
  `set role` to it and then `gnomon.begin` another run's branch. Treat it as confined to the runs
  role, not to one run.
- The runs role can `alter role current_user set ...`. Such a setting applies to every new runs
  connection and survives any reset of the session: one statement such as
  `alter role current_user set default_transaction_read_only = on` makes every later run fail
  until an admin runs `alter role <runs login> reset all`.
- The runs login can list every branch name and task.
- A run's statement can call `gnomon.checkpoint(name, 'main')` and create a tag on main.
- Error kinds on the runs login are advisory: a statement can raise any SQLSTATE, and put a
  `[gnomon:<slug>]` code in its hint (the crate reads the code from the hint only, as the spec
  says, so a slug in a message is ignored). A raised 40001 is retried like a real one. A merge conflict is only ever read from the merge itself.

## Developing

```bash
docker compose up -d
```

```bash
GNOMON_TEST_URL=postgres://postgres@127.0.0.1:54329/postgres cargo test
```

```bash
GNOMON_TEST_URL=postgres://postgres@127.0.0.1:54330/postgres cargo test
```

Port 54329 runs with gnomon's C module and 54330 runs the SQL core only. Without `GNOMON_TEST_URL`,
the database tests return early and pass (`--nocapture` shows their `SKIPPED` line). `spec/` is vendored from
[bitsinfra/gnomon](https://github.com/bitsinfra/gnomon) v1.0.1 (PostgreSQL License, `spec/LICENSE`).
