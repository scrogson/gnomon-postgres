# Gnomon SDK specification

One specification, the same names in every language, and a conformance suite every SDK passes
(`conformance/*.json`). Every method is one SQL function call: the database is the only place with
logic. Version 1, against the SQL API `gnomon.api_version() = 2`.

## Two logins

- **app**: the application's own login. Starts runs (a member of `gnomon_runner`), reads their
  diffs and summaries, accepts them (a member of `gnomon_merger`, or covered by a merge policy),
  reviews change requests.
- **runs**: one confined login shared by every agent run, holding no grant on the base tables. Named
  once in the database: `gnomon.set('runs_role', '<login>')`, after `gnomon.confine('<login>')`.

## The envelope

Every agent statement on a run is sent on the runs login as:

```
BEGIN
SELECT gnomon.begin($branch)                         -- binds this transaction to the run
SELECT gnomon.note($actor, $reason, $meta::jsonb)    -- attribution: history, blame, the log
SELECT set_config('statement_timeout', $ms, true), set_config('lock_timeout', $ms, true)
<the statement, by the extended protocol, one statement>
COMMIT
```

A second statement is refused (by PostgreSQL where the driver uses the extended protocol, and by
the SDK's own splitter for every driver). A run cannot read or write another run's branch, even on
the same login: `gnomon.begin` binds the transaction to one run.

## Methods

| Layer | Method | SQL |
|---|---|---|
| connect | version check | `select gnomon.api_version()` (supported: 2) |
| runs | `startRun({task, actor, kind, ttl, parent})` | `gnomon.run_start(task, actor, kind, ttl, parent)` (app) |
| | `run.sql(statement, params)` | the envelope (runs) |
| | `run.transaction(fn)` | the envelope around several statements |
| | `run.diff()` | `gnomon.diff(branch)` (app) |
| | `run.summary()` | `gnomon.run_summary(branch)` (app) |
| | `run.checks()` | `gnomon.run_checks(branch)` (app) |
| | `run.merge()` | `gnomon.merge(branch, close => true)` (app) |
| | `run.submit({title, body})` | `gnomon.open_change_request(branch, title, body)` (app) |
| | `run.discard()` | `gnomon.run_end(branch, false)` (app) |
| | `run.checkpoint(name)`, `run.rewind(name)` | `gnomon.checkpoint`, `gnomon.rewind` |
| | `run.dryRun(statements)` | `gnomon.dry_run(statements, branch)` |
| review | `requests.open()` | `gnomon.change_request` where open |
| | `request.status()` | `gnomon.change_request_status(id)` |
| | `approve(body)`, `reject(note)`, `comment(body, at)` | `gnomon.review`, `gnomon.comment` |
| | `merge()`, `undo()`, `wait(timeout)` | `gnomon.merge`, `gnomon.undo_merge`, `gnomon.wait_for_review` |
| core | `branches.create/drop/rebase/list` | `gnomon.create_branch`, `drop_branch`, `rebase`, `gnomon.branches` |
| | `history(table, key, {branch})`, `blame` | `gnomon.history`, `gnomon.blame` |
| | `revert(table, at, {keys, filter, branch})` | `gnomon.revert` |
| | `status()`, `doctor()` | `gnomon.status()`, `gnomon.doctor()` |

Leaving a run's scope (`await using`, `with`) without `merge`, `submit` or `discard` discards it.

## Errors

A base `GnomonError` with `code` (the slug in the hint, `[gnomon:<slug>]`), `sqlstate`, `hint` and
the driver's error as `cause`. Subclasses by what callers branch on:

| Class | When |
|---|---|
| `SerializationError` | SQLSTATE 40001 |
| `PermissionError` | 42501 |
| `BranchClosedError` | 25006, or slug `branch_closed` |
| `StatementTimeoutError` | 57014 |
| `UnsupportedOnBranchError` | 42P10 (an upsert on a branch), slug `sql_only_dml` |
| `NotRoutedError` | slug `not_routed` |
| `MergeConflictError` | a merge stopped on conflicts; carries them |
| `ChecksFailedError` | 23514 (a check failed inside a merge) |
| `OverQuotaError` | 53400 |
| `IncompatibleVersionError` | the extension's API version is outside the SDK's range |

`run.sql` retries a statement that failed with 40001, with backoff, up to three times: the envelope
held nothing else. `run.transaction(fn)` retries by running `fn` again. Nothing else is retried.

## Values

A diff carries values as JSON. Integers beyond 2^53 are read from the JSON text, not through a
float: `bigint` becomes `BigInt` (or a string, by option); a decimal whose text a float would not
reproduce stays a string. Python decodes to `int` and `Decimal`.

## Tools for models

`tools.json` holds the tools every framework adapter returns, bound to one run: `sql`, `describe`,
`diff`, `dry_run`, `checkpoint`, `rewind`, `done`, with descriptions written for models. Each SDK
carries a copy; the conformance suite checks that it equals this file.

## Snapshots

A diff normalized for a stored file. Each table's changed keys are replaced by placeholders
(`<accounts:1>`, `<accounts:2>`, ...) in ascending key order (numbers numerically, text by code
point, a composite key column by column): generated keys differ between runs, their order does not.
A foreign-key column whose value is a placeholder's key shows the placeholder. Named columns are
ignored. One line per changed field (an insert: one per column; a delete: one line), sorted by
table, placeholder number, then text:

```
public.accounts <accounts:1> insert email = "a@x.io"
public.accounts <accounts:1> insert id = <accounts:1>
public.orders <orders:1> update total: 10 -> 12
public.orders <orders:2> delete
```

Values are JSON text with object keys sorted.

`snapshot/*.json` holds inputs and expected outputs both SDKs check.

## Conformance scenarios

`conformance/*.json` are scenarios as data: `setup` SQL run as a superuser, then `steps`, each a
`call` (`startRun`, `run.sql`, `app.sql`, `run.diff`, `run.summary`, `run.merge`, `run.discard`)
with `args` and an `expect`: `error` (an error class of the Errors section), `rowCount`, `rows`,
`length`, or a dotted path into the result. `"sqlOnly": true` skips a step when the server loads
the C module, where it behaves differently by design (a schema-qualified name is routed to the
branch instead of refused). A binding is conformant when every scenario passes through every
driver adapter it ships, against the SQL core and the C module.

## A connection of the run's own

`run.url(base?)` and `run.connection(...)` hand a tool a connection pinned to one run. The shared
runs login cannot do it (it binds each transaction to one run with `gnomon.begin(run)`, so a
pinned session would not carry the binding), so they call `gnomon.run_login(run)`: a login role of
the run's own, a member of the runs role, confined to the run's branch, whose sessions start on
the branch through role defaults. `run.url(base)` puts its role and password into `base` (the
SDK's `url` option or the argument; for a socket directory, as `user` and `password` query
parameters, since libpq takes the user from there); `run.connection(connect, base?)` opens one
with the caller's driver. A second call rotates the password. `run_end` ends the login's sessions
and drops it. Behind a transaction pooler the branch still comes from the login's role defaults
(pools are per user), not from session state.
