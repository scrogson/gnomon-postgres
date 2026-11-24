//! Findings of the security audit: each test is one finding, attempted the way a model would.

mod common;

use std::time::Duration;

use gnomon::tools::run_tool;
use std::sync::{Arc, Mutex};

use gnomon::{ErrorKind, Gnomon, Login, StartRun};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

fn setup() -> Vec<String> {
    [
        "create table acct (id int primary key, note text not null)",
        "insert into acct values (1, 'real'), (2, 'two')",
        "select gnomon.track('acct')",
        // tracked after acct, though it sorts before it
        "create table aaa (id int primary key, v int not null)",
        "insert into aaa values (1, 1)",
        "select gnomon.track('aaa')",
        "create table doc (id int primary key, title text not null, body jsonb)",
        "select gnomon.track('doc')",
    ]
    .map(String::from)
    .to_vec()
}

macro_rules! fixture {
    ($name:expr) => {{
        let Some(server) = common::server() else {
            return;
        };
        common::Fixture::new(&server, $name, &setup()).await
    }};
}

/// A client whose runs pool has one connection, so every run shares it.
fn one_connection(fx: &common::Fixture) -> Gnomon {
    let mut u = url::Url::parse(&fx.server).unwrap();
    u.set_path(&fx.db);
    u.set_username(&fx.runs_role).unwrap();
    let runs = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(u.as_str())
        .unwrap();
    Gnomon::builder(fx.app.clone())
        .runs(runs)
        .statement_timeout(Duration::from_millis(1500))
        .build()
}

/// One run's session state (a temp view shadowing a table, a read-only default, an advisory
/// lock) reached the next run on the same pooled connection.
#[tokio::test]
async fn nothing_crosses_from_one_run_to_the_next() {
    let fx = fixture!("sec_session");
    let g = one_connection(&fx);
    let a = g.start_run(StartRun::new().task("a")).await.unwrap();
    // the C module refuses some of these; whatever is accepted must not reach run b
    for s in [
        // hide the planted objects from a check that reads the catalogs unqualified
        "do $$ begin create temp view pg_class as select * from pg_catalog.pg_class where relnamespace <> pg_my_temp_schema(); create temp view pg_type as select * from pg_catalog.pg_type where typnamespace <> pg_my_temp_schema(); end $$",
        "create temp view acct as select 1 as id, 'planted'::text as note",
        "create temp table acct (id int, note text)",
        "set default_transaction_read_only = on",
        "set search_path = pg_temp, public",
        "select pg_advisory_lock(42)",
        "declare cur cursor with hold for select 1",
    ] {
        let _ = a.sql(s).await;
    }
    let b = g.start_run(StartRun::new().task("b")).await.unwrap();
    for _ in 0..2 {
        let r = b.sql("select note from acct where id = 1").await.unwrap();
        assert_eq!(r.json_rows().unwrap()[0]["note"], "real");
    }
    b.sql("update acct set note = 'b' where id = 2")
        .await
        .unwrap();
    assert_eq!(b.diff().await.unwrap().len(), 1);
    let locked = b
        .sql("select count(*)::int8 as n from pg_locks where locktype = 'advisory' and pid = pg_backend_pid()")
        .await
        .unwrap();
    assert_eq!(locked.json_rows().unwrap()[0]["n"], 0);
    a.discard().await.unwrap();
    b.discard().await.unwrap();
    fx.cleanup().await;
}

/// dry_run ran a model's statements as the app login, without a timeout.
#[tokio::test]
async fn dry_run_is_confined_to_the_run() {
    let fx = fixture!("sec_dry_run");
    let g = one_connection(&fx);
    let run = g.start_run(StartRun::new().task("dry")).await.unwrap();
    run.sql("update acct set note = 'x' where id = 1")
        .await
        .unwrap();
    let o = run_tool(
        &run,
        "dry_run",
        &json!({"statements": ["update acct set note = current_user where id = 1"]}),
    )
    .await;
    assert!(!o.is_error, "{}", o.text);
    let v = o.value.unwrap();
    assert_eq!(v["ok"], true, "{v}");
    let d = &v["diff"][0];
    assert_eq!(
        d["table"], "acct",
        "named as upstream and run.diff() name it: {v}"
    );
    assert_eq!(v["summary"][0]["table"], "acct", "{v}");
    assert_eq!(
        d["old"], "x",
        "only what the statements change, from the run's value: {v}"
    );
    assert_eq!(d["new"], fx.runs_role.as_str(), "not the app login: {v}");
    assert_eq!(v["summary"][0]["updates"], 1, "{v}");
    assert_eq!(run.diff().await.unwrap()[0].new, json!("x"), "nothing kept");

    for stmts in [
        vec!["select pg_sleep(5)"],
        vec!["set statement_timeout = 0", "select pg_sleep(5)"],
        vec!["set local statement_timeout = 0", "select pg_sleep(5)"],
    ] {
        let stmts: Vec<String> = stmts.into_iter().map(String::from).collect();
        let v = run.dry_run(&stmts, false).await.unwrap();
        assert_eq!(v["ok"], false, "{stmts:?}: {v}");
        assert_eq!(
            v["sqlstate"], "57014",
            "the run's statement timeout applies: {stmts:?}: {v}"
        );
    }

    // tables in tracking order, as gnomon.dry_run lists them
    let v = run
        .dry_run(
            &[
                "update aaa set v = 2 where id = 1".into(),
                "update acct set note = 'y' where id = 2".into(),
            ],
            false,
        )
        .await
        .unwrap();
    let tables: Vec<&str> = v["diff"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["table"].as_str().unwrap())
        .collect();
    assert_eq!(tables, ["acct", "aaa"], "{v}");

    for s in [
        "commit",
        "/* hi */ COMMIT",
        "rollback",
        "begin",
        "\x0bcommit",
        "-- x\rcommit",
        ";commit",
        "\x0c end transaction",
    ] {
        let v = run
            .dry_run(
                &["update acct set note = 'dry' where id = 2".into(), s.into()],
                false,
            )
            .await
            .unwrap();
        assert_eq!(v["ok"], false, "{s:?}: {v}");
        let d = run.diff().await.unwrap();
        assert_eq!(d.len(), 1, "{s:?}: the dry run kept nothing: {d:?}");
    }
    // after all that the connection is still good
    assert_eq!(run.sql("select 1").await.unwrap().row_count, 1);
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// COMMIT inside transaction() ended the envelope; what followed ran outside it.
#[tokio::test]
async fn transaction_refuses_transaction_control() {
    let fx = fixture!("sec_tx_control");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("tx")).await.unwrap();
    for s in [
        "commit",
        "  -- x\n end",
        "rollback work",
        "abort",
        "start transaction",
        "prepare transaction 'x'",
        "\x0bcommit",
        "-- x\rcommit",
        ";commit",
        "; ;\x0c-- y\r/* z */ END TRANSACTION",
    ] {
        let e = run
            .transaction(None, |tx| {
                let s = s.to_string();
                Box::pin(async move {
                    tx.sql("update acct set note = 'kept?' where id = 1")
                        .await?;
                    tx.sql(&s).await?;
                    tx.sql("update acct set note = 'after' where id = 2")
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some("transaction_control"), "{s:?}: {e}");
        assert!(run.diff().await.unwrap().is_empty(), "{s:?}: nothing kept");
    }
    assert!(run.diff().await.unwrap().is_empty(), "nothing kept");
    // a single statement has no use for it either (PREPARE TRANSACTION would outlive the pool)
    for s in ["prepare transaction 'x'", "commit", "\x0bbegin"] {
        let e = run.sql(s).await.unwrap_err();
        assert_eq!(e.code(), Some("transaction_control"), "{s:?}: {e}");
    }
    // while statements that only mention such words still run
    for s in [
        "-- commit\nselect 1",
        "/* begin */ select 1",
        ";select 1",
        "do $$ begin null; end $$",
        "select 'commit' as begin",
    ] {
        run.sql(s).await.unwrap_or_else(|e| panic!("{s:?}: {e}"));
    }
    // savepoints inside the transaction are fine
    run.transaction(None, |tx| {
        Box::pin(async move {
            tx.sql("savepoint s").await?;
            tx.sql("update acct set note = 'gone' where id = 1").await?;
            tx.sql("rollback to savepoint s").await?;
            tx.sql("update acct set note = 'kept' where id = 2").await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let d = run.diff().await.unwrap();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].new, json!("kept"));
    // an error the closure swallows must not turn into a reported success
    let e = run
        .transaction(None, |tx| {
            Box::pin(async move {
                let _ = tx.sql("select 1/0").await;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert_eq!(e.sqlstate(), Some("25P02"), "{e}");
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// The row cap let 1000 rows of 400 KB each through.
#[tokio::test]
async fn rows_are_capped_by_bytes_too() {
    let fx = fixture!("sec_bytes");
    let g = Gnomon::builder(fx.app.clone())
        .runs(fx.runs.clone())
        .max_bytes(1000)
        .build();
    let run = g.start_run(StartRun::new().task("bytes")).await.unwrap();
    let r = run
        .sql("select repeat('x', 400) as s from generate_series(1, 10)")
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert!(r.truncated);
    assert_eq!(r.row_count, 10);
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// Request::merge returned Ok when the merge stopped on conflicts.
#[tokio::test]
async fn request_merge_reports_conflicts() {
    let fx = fixture!("sec_req_merge");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("conflict")).await.unwrap();
    run.sql("update acct set note = 'run' where id = 1")
        .await
        .unwrap();
    let req = run.submit(None).await.unwrap();
    g.app_sql(
        "update acct set note = 'main' where id = 1",
        Default::default(),
    )
    .await
    .unwrap();
    let reviewer = fx.reviewer().await;
    reviewer
        .requests()
        .get(req.id())
        .approve(Some("ok"))
        .await
        .unwrap();
    let e = req.merge().await.unwrap_err();
    assert!(
        matches!(e.kind(), ErrorKind::MergeConflict { conflicts, .. } if !conflicts.is_empty()),
        "{e}"
    );
    fx.cleanup().await;
}

/// Cancelled calls closed their connections, leaving their statements running on the server:
/// more runs-login backends than the pool allows.
#[tokio::test]
async fn cancelled_calls_stay_within_the_pool() {
    let fx = fixture!("sec_cancel");
    let g = one_connection(&fx);
    let run = g.start_run(StartRun::new().task("cancel")).await.unwrap();
    for _ in 0..5 {
        let _ =
            tokio::time::timeout(Duration::from_millis(100), run.sql("select pg_sleep(1)")).await;
    }
    let mut admin = fx.admin().await;
    let n: i64 = sqlx::query_scalar("select count(*) from pg_stat_activity where usename = $1")
        .bind(&fx.runs_role)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert!(n <= 1, "{n} runs-login backends for a pool of one");
    // and the connection still works afterwards
    assert_eq!(run.sql("select 1").await.unwrap().row_count, 1);
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// A temporary view named like a catalog broke the session reset for good.
#[tokio::test]
async fn a_shadowed_catalog_cannot_break_the_connection() {
    let fx = fixture!("sec_shadow");
    let g = one_connection(&fx);
    let a = g.start_run(StartRun::new().task("a")).await.unwrap();
    // one statement, so the next run gets the very session that holds all of them
    a.sql(
        "do $$ begin \
         create temp view pg_ts_dict as select 1 as x; \
         create temp view pg_conversion as select 1 as x; \
         create temp view pg_statistic_ext as select 1 as x; \
         create temp view pg_extension as select 1 as x; \
         end $$",
    )
    .await
    .unwrap();
    let b = g.start_run(StartRun::new().task("b")).await.unwrap();
    for _ in 0..2 {
        assert_eq!(b.sql("select 1").await.unwrap().row_count, 1);
    }
    a.discard().await.unwrap();
    b.discard().await.unwrap();
    fx.cleanup().await;
}

/// A model's statement could pass itself off as a gnomon refusal by putting a slug in its message.
#[tokio::test]
async fn error_kinds_come_from_the_hint() {
    let fx = fixture!("sec_forge");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("forge")).await.unwrap();
    for s in [
        "select '[gnomon:not_routed]'::int",
        "select '[gnomon:over_quota]'::int",
    ] {
        let e = run.sql(s).await.unwrap_err();
        assert_eq!(e.kind(), &ErrorKind::Other, "{s}: {e}");
        assert_eq!(e.code(), None, "{s}: {e}");
    }
    // gnomon's own refusals still carry their slug
    let other = g.start_run(StartRun::new().task("other")).await.unwrap();
    let e = run
        .sql_with("select gnomon.begin($1)", gnomon::args![other.branch()])
        .await
        .unwrap_err();
    assert!(e.code().is_some(), "{e}");
    other.discard().await.unwrap();
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// After `done`, the tools still rewound, dry-ran and opened a second change request.
#[tokio::test]
async fn a_submitted_run_takes_no_more_tools() {
    let fx = fixture!("sec_done");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("done")).await.unwrap();
    run.sql("update acct set note = 'n' where id = 1")
        .await
        .unwrap();
    run.checkpoint("c").await.unwrap();
    // two dones at once, as a model's parallel tool calls send them
    let (one, two) = (json!({"summary": "one"}), json!({"summary": "two"}));
    let (a, b) = tokio::join!(run_tool(&run, "done", &one), run_tool(&run, "done", &two));
    assert!(!a.is_error && !b.is_error, "{} / {}", a.text, b.text);
    let c = run_tool(&run, "done", &json!({"summary": "three"})).await;
    assert!(!c.is_error, "{}", c.text);
    let id = |o: &gnomon::tools::ToolOutput| o.value.as_ref().unwrap()["change_request"].clone();
    assert_eq!(id(&a), id(&b));
    assert_eq!(id(&a), id(&c));
    let mut admin = fx.admin().await;
    let n: i64 = sqlx::query_scalar("select count(*) from gnomon.change_request r join gnomon.branch b on b.id = r.branch where b.name = $1")
        .bind(run.branch())
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(n, 1, "one change request per run");
    for (tool, input) in [
        ("rewind", json!({"name": "c"})),
        ("checkpoint", json!({"name": "d"})),
        ("dry_run", json!({"statements": ["select 1"]})),
        ("describe", json!({})),
        ("sql", json!({"statement": "select 1"})),
    ] {
        let o = run_tool(&run, tool, &input).await;
        assert!(o.is_error, "{tool} after done: {}", o.text);
    }
    assert_eq!(
        run.diff().await.unwrap().len(),
        1,
        "the submitted change stays"
    );
    fx.cleanup().await;
}

/// checkpoint and rewind ran on the app login, with no statement timeout.
#[tokio::test]
async fn checkpoints_go_through_the_runs_login() {
    let fx = fixture!("sec_checkpoint");
    let seen: Arc<Mutex<Vec<(Login, String)>>> = Arc::default();
    let s2 = seen.clone();
    let g = Gnomon::builder(fx.app.clone())
        .runs(fx.runs.clone())
        .on_statement(move |e| {
            s2.lock().unwrap().push((e.login, e.statement.to_string()));
        })
        .build();
    let run = g.start_run(StartRun::new().task("cp")).await.unwrap();
    run.sql("update acct set note = 'a' where id = 1")
        .await
        .unwrap();
    run.checkpoint("before b").await.unwrap();
    run.sql("update acct set note = 'b' where id = 2")
        .await
        .unwrap();
    run.rewind("before b").await.unwrap();
    let d = run.diff().await.unwrap();
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!(d[0].new, json!("a"));
    let seen = seen.lock().unwrap().clone();
    for f in ["gnomon.checkpoint", "gnomon.rewind"] {
        let logins: Vec<Login> = seen
            .iter()
            .filter(|(_, s)| s.contains(f))
            .map(|(l, _)| *l)
            .collect();
        assert_eq!(logins, [Login::Runs], "{f}: {seen:?}");
    }
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// A value nested deeper than the parser takes made the whole diff fail, while summary passed.
#[tokio::test]
async fn deep_json_does_not_hide_a_diff() {
    let fx = fixture!("sec_deep_diff");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("deep")).await.unwrap();
    let deep = "(repeat('[', 500) || '1' || repeat(']', 500))::jsonb";
    run.sql(&format!("insert into doc values (1, 'visible', {deep})"))
        .await
        .unwrap();
    let check = |d: &[gnomon::Change]| {
        assert_eq!(d.len(), 1, "{d:?}");
        let img = &d[0].new;
        assert_eq!(img["title"], "visible", "other columns intact: {img}");
        assert_eq!(img["id"], 1);
        let mut v = &img["body"];
        let mut depth = 0;
        while let Some(a) = v.as_array() {
            v = &a[0];
            depth += 1;
        }
        assert!(depth > 300, "kept as deep as the cap allows: {depth}");
        assert!(
            v.as_str().is_some_and(|s| s.contains("::text")),
            "the cut is marked: {v}"
        );
    };
    check(&run.diff().await.unwrap());
    let v = run
        .dry_run(
            &["update doc set title = 'still' where id = 1".into()],
            false,
        )
        .await
        .unwrap();
    assert_eq!(v["ok"], true, "{v}");
    let d: Vec<gnomon::Change> = serde_json::from_value(v["diff"].clone()).unwrap();
    assert_eq!(d.len(), 1, "{v}");
    assert_eq!(d[0].column.as_deref(), Some("title"), "{v}");
    // a change past the cap still shows, as a changed placeholder
    let v = run
        .dry_run(
            &["update doc set body = (repeat('[', 500) || '2' || repeat(']', 500))::jsonb where id = 1"
                .into()],
            false,
        )
        .await
        .unwrap();
    let d: Vec<gnomon::Change> = serde_json::from_value(v["diff"].clone()).unwrap();
    assert_eq!(d.len(), 1, "{v}");
    assert_ne!(d[0].old, d[0].new, "{v}");
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// Run::url rewrote the base's query: `%20` came back as `+`, which libpq does not decode.
#[tokio::test]
async fn run_url_keeps_the_base_query() {
    let fx = fixture!("sec_url_query");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("url")).await.unwrap();
    let mut base = url::Url::parse(&fx.server).unwrap();
    base.set_path(&fx.db);
    base.set_username("").unwrap();
    base.set_query(Some(
        "options=-c%20statement_timeout%3D4321ms&%75ser=postgres",
    ));
    let u = run.url(Some(base.as_str())).await.unwrap();
    assert!(u.contains("options=-c%20statement_timeout%3D4321ms"), "{u}");
    assert!(!u.contains("postgres&") && !u.ends_with("=postgres"), "{u}");
    let mut conn = run.connect(Some(base.as_str())).await.unwrap();
    let t: String = sqlx::query_scalar("show statement_timeout")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "4321ms");
    let who: String = sqlx::query_scalar("select current_user::text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_ne!(who, "postgres");
    drop(conn);
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// A statement in flight when the run was submitted wrote to the branch after the request opened.
#[tokio::test]
async fn submit_waits_for_calls_in_flight() {
    let fx = fixture!("sec_in_flight");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("late")).await.unwrap();
    let r2 = run.clone();
    let write = tokio::spawn(async move {
        let r = r2
            .sql("update acct set note = 'late' where id = 2 and pg_sleep(1) is not null")
            .await;
        (r, std::time::Instant::now())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = run.submit(None).await.unwrap();
    let submitted = std::time::Instant::now();
    let (r, wrote) = write.await.unwrap();
    assert_eq!(r.unwrap().row_count, 1);
    assert!(
        wrote <= submitted,
        "the request opened only after the write ended"
    );
    // and nothing writes once it is open
    assert!(
        run.sql("update acct set note = 'x' where id = 1")
            .await
            .is_err()
    );
    let _ = req;
    fx.cleanup().await;
}
