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

### What a run does

| | |
|---|---|
| `run.sql(s)`, `run.sql_with(s, args![..])`, `run.sql_opts(s, args, SqlOptions { reason, timeout, max_rows })` | One statement on the branch. Rows are capped (`truncated`). A 40001 is retried 3 times with backoff. |
| `run.transaction(reason, \|tx\| Box::pin(async move { .. }))` | Several statements in one envelope, all or nothing, rows not capped. Retried by running the closure again. |
| `run.diff()`, `run.summary()`, `run.checks()` | What changed, per field; changes, checks, conflicts and warnings in one call. |
| `run.merge()`, `run.submit(title)`, `run.discard()` | Into the real data, to a person, or away. A conflict is `ErrorKind::MergeConflict { merge_id, conflicts }`. |
| `run.checkpoint(name)`, `run.rewind(name)`, `run.dry_run(&stmts, checks)` | Savepoints across calls, and trying statements without keeping them. |
| `run.url(base)`, `run.connect(base)` | A connection string (URL or libpq keywords), or an open `PgConnection` (URL base only), for a login of the run's own, confined to its branch. |
| `run.describe(schema)` | The database as the run sees it. |

`SqlResult.rows` are sqlx `PgRow`s. `json_rows()` (or `gnomon::row_to_json`) turns them into JSON
objects: booleans, integers, floats, numeric, text types, enums, json, uuid, dates, times,
intervals, bytea, and arrays of the common scalars. Any other type (inet, money, ranges, ...) is
an error telling you to cast it with `::text`; `row_to_json_lossy` puts a placeholder string there
instead. The `sql` tool uses the lossy form, so a statement that committed is never reported to a
model as failed.

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

`gnomon::Error` has `kind()`, `code()` (the stable `[gnomon:<slug>]`), `sqlstate()`, `hint()` and
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
error rather than a risk of overflowing the stack.

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
