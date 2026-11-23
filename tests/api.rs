//! The API beyond the conformance scenarios, against a real server (`GNOMON_TEST_URL`).

mod common;

use std::time::Duration;

use gnomon::testing::{assert_changed, assert_no_changes, gnomon_diff, with_scratch};
use gnomon::tools::run_tool;
use gnomon::{ErrorKind, RunState, StartRun, args, row_to_json};
use serde_json::json;
use sqlx::Row;

fn setup() -> Vec<String> {
    [
        "create table customers (id bigint primary key, email text not null)",
        "create table orders (id bigint primary key, customer bigint references customers, total numeric(12,2) not null, big bigint not null default 0)",
        "insert into customers values (1, 'a@x.io'), (2, 'b@x.io')",
        "insert into orders values (10, 1, 10.00, 0), (11, 2, 20.50, 0)",
        "select gnomon.track('customers')",
        "select gnomon.track('orders')",
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

#[tokio::test]
async fn lossless_values_in_a_diff() {
    let fx = fixture!("api_lossless");
    let g = fx.gnomon();
    let run = g
        .start_run(StartRun::new().task("big numbers"))
        .await
        .unwrap();
    run.sql("update orders set big = 9007199254740993, total = 1.10 where id = 10")
        .await
        .unwrap();
    let d = run.diff().await.unwrap();
    let big = d
        .iter()
        .find(|c| c.column.as_deref() == Some("big"))
        .expect("big changed");
    assert_eq!(big.new.to_string(), "9007199254740993");
    let total = d
        .iter()
        .find(|c| c.column.as_deref() == Some("total"))
        .expect("total changed");
    assert_eq!(
        total.new,
        json!("1.10"),
        "a decimal a float would not reproduce stays text"
    );
    // and in rows read back on the run
    let r = run
        .sql("select big, total from orders where id = 10")
        .await
        .unwrap();
    let row = &r.json_rows().unwrap()[0];
    assert_eq!(row["big"].to_string(), "9007199254740993");
    assert_eq!(row["total"], json!("1.10"));
    run.discard().await.unwrap();
    fx.cleanup().await;
}

#[tokio::test]
async fn parameters_truncation_and_transactions() {
    let fx = fixture!("api_tx");
    let g = fx.gnomon();
    let run = g
        .start_run(StartRun::new().task("tx").actor("tester"))
        .await
        .unwrap();
    let r = run
        .sql_with("select email from customers where id = $1", args![2_i64])
        .await
        .unwrap();
    assert_eq!(r.rows[0].get::<String, _>(0), "b@x.io");
    assert_eq!(r.fields, ["email"]);

    let many = run
        .sql_opts(
            "select g from generate_series(1, 50) g",
            Default::default(),
            gnomon::SqlOptions {
                max_rows: Some(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(many.truncated);
    assert_eq!(many.rows.len(), 10);
    assert_eq!(many.row_count, 50);

    run.transaction(Some("move money"), |tx| {
        Box::pin(async move {
            tx.sql("update orders set total = total - 5 where id = 11")
                .await?;
            tx.sql_with(
                "update orders set total = total + $1 where id = 10",
                args![5_i32],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let d = run.diff().await.unwrap();
    assert_eq!(d.len(), 2, "{d:?}");

    // a failure inside the transaction rolls all of it back
    let e = run
        .transaction(None, |tx| {
            Box::pin(async move {
                tx.sql("update orders set total = 0 where id = 10").await?;
                tx.sql("select 1; select 2").await?;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some("one_statement"));
    assert_eq!(run.diff().await.unwrap().len(), 2);

    // attribution: the note's actor and reason reach history
    let h = g
        .history("orders", &json!({"id": 10}), Some(run.branch()))
        .await
        .unwrap();
    assert!(
        h.iter()
            .any(|v| v["actor"] == "tester" && v["reason"] == "move money"),
        "{h:?}"
    );
    run.discard().await.unwrap();
    assert_eq!(run.state(), RunState::Discarded);
    fx.cleanup().await;
}

#[tokio::test]
async fn with_run_discards_unless_finished() {
    let fx = fixture!("api_with_run");
    let g = fx.gnomon();
    let mut seen = String::new();
    let out: gnomon::Result<()> = g
        .with_run(StartRun::new().task("abandoned"), |run| {
            seen = run.branch().to_string();
            async move {
                run.sql("delete from orders where id = 11").await?;
                Err(gnomon::Error::new(ErrorKind::Other, "the agent gave up"))
            }
        })
        .await;
    assert!(out.is_err());
    let status: Option<String> =
        sqlx::query_scalar("select status from gnomon.branches where name = $1")
            .bind(&seen)
            .fetch_optional(&fx.app)
            .await
            .unwrap();
    assert!(!seen.is_empty());
    assert!(
        status.is_none() || status.as_deref() == Some("closed"),
        "with_run should have discarded the run, branch is {status:?}"
    );

    let merged = g
        .with_run(StartRun::new().task("kept"), |run| async move {
            run.sql("update customers set email = 'c@x.io' where id = 1")
                .await?;
            run.merge().await
        })
        .await
        .unwrap();
    assert_eq!(merged.status, "applied");
    let email: String = sqlx::query_scalar("select email from customers where id = 1")
        .fetch_one(&fx.app)
        .await
        .unwrap();
    assert_eq!(email, "c@x.io");
    fx.cleanup().await;
}

#[tokio::test]
async fn review_flow_merge_and_undo() {
    let fx = fixture!("api_review");
    let g = fx.gnomon();
    let run = g
        .start_run(StartRun::new().task("Refund order 10"))
        .await
        .unwrap();
    run.sql("update orders set total = 0 where id = 10")
        .await
        .unwrap();
    let summary = run.summary().await.unwrap();
    assert_eq!(summary.changes.rows, 1);
    assert!(summary.checks.passed);
    assert!(run.checks().await.is_ok());

    let req = run.submit(None).await.unwrap();
    assert_eq!(run.state(), RunState::Submitted);
    let open = g.requests().open().await.unwrap();
    assert!(open.iter().any(|r| r.id() == req.id()));
    let status = req.status().await.unwrap();
    assert_eq!(status["title"], "Refund order 10", "{status}");
    assert_eq!(req.diff().await.unwrap().len(), 1);
    let cid = req
        .comment(
            "why zero?",
            gnomon::CommentAt {
                table: Some("orders".into()),
                key: Some(json!({"id": 10})),
                column: Some("total".into()),
            },
        )
        .await
        .unwrap();
    assert!(cid > 0);

    // the opener may not approve their own request; a second person does, through the client
    let own = req.approve(Some("mine")).await.unwrap_err();
    assert_eq!(own.code(), Some("author_change_request_cannot"), "{own}");
    let reviewer = fx.reviewer().await;
    let theirs = reviewer.requests().get(req.id());
    theirs.approve(Some("ok")).await.unwrap();
    assert_eq!(theirs.status().await.unwrap()["status"], "approved");
    let w = req.wait(Duration::from_millis(100)).await.unwrap();
    assert!(w.is_object(), "{w}");
    let merge_id = req.merge().await.unwrap();
    assert!(merge_id > 0);
    let total: String = sqlx::query_scalar("select total::text from orders where id = 10")
        .fetch_one(&fx.app)
        .await
        .unwrap();
    assert_eq!(total, "0.00");

    req.undo().await.unwrap();

    // reject: the request stays unmerged
    let run2 = g
        .start_run(StartRun::new().task("Refund order 11"))
        .await
        .unwrap();
    run2.sql("update orders set total = 0 where id = 11")
        .await
        .unwrap();
    let req2 = run2.submit(None).await.unwrap();
    reviewer
        .requests()
        .get(req2.id())
        .reject(Some("no"))
        .await
        .unwrap();
    let s2 = req2.status().await.unwrap();
    assert_ne!(s2["status"], "merged", "{s2}");
    assert!(s2.to_string().contains("reject"), "{s2}");
    let total: String = sqlx::query_scalar("select total::text from orders where id = 10")
        .fetch_one(&fx.app)
        .await
        .unwrap();
    assert_eq!(total, "10.00");
    fx.cleanup().await;
}

#[tokio::test]
async fn checkpoint_rewind_dry_run_describe() {
    let fx = fixture!("api_checkpoint");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("cp")).await.unwrap();
    run.sql("update orders set total = 1 where id = 10")
        .await
        .unwrap();
    run.checkpoint("one").await.unwrap();
    run.sql("update orders set total = 2 where id = 11")
        .await
        .unwrap();
    assert_eq!(run.diff().await.unwrap().len(), 2);
    run.rewind("one").await.unwrap();
    assert_eq!(run.diff().await.unwrap().len(), 1);

    let dr = run
        .dry_run(&["delete from orders where id = 11".to_string()], true)
        .await
        .unwrap();
    assert_eq!(dr["ok"], true, "{dr}");
    assert_eq!(
        run.diff().await.unwrap().len(),
        1,
        "a dry run keeps nothing"
    );

    let d = run.describe(None).await.unwrap();
    assert!(d["tables"].as_array().is_some_and(|t| !t.is_empty()), "{d}");
    run.discard().await.unwrap();
    fx.cleanup().await;
}

#[tokio::test]
async fn a_connection_of_the_runs_own() {
    let fx = fixture!("api_url");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("url")).await.unwrap();
    run.sql("update customers set email = 'own@x.io' where id = 2")
        .await
        .unwrap();
    let mut conn = run.connect(None).await.unwrap();
    let email: String = sqlx::query_scalar("select email from customers where id = 2")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(email, "own@x.io");
    drop(conn);
    run.discard().await.unwrap();
    fx.cleanup().await;
}

#[tokio::test]
async fn tools_for_models() {
    let fx = fixture!("api_tools");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("tools")).await.unwrap();
    let o = run_tool(
        &run,
        "sql",
        &json!({"statement": "update orders set total = 3 where id = 10", "reason": "test"}),
    )
    .await;
    assert!(!o.is_error, "{}", o.text);
    assert_eq!(o.value.unwrap()["rowCount"], 1);
    let o = run_tool(&run, "sql", &json!({"statement": "select 1; select 2"})).await;
    assert!(o.is_error && o.text.contains("one statement"), "{}", o.text);
    let o = run_tool(&run, "diff", &json!({})).await;
    assert!(o.text.contains("\"column\":\"total\""), "{}", o.text);
    for (name, input) in [
        ("describe", json!({})),
        ("checkpoint", json!({"name": "a"})),
        ("rewind", json!({"name": "a"})),
        ("dry_run", json!({"statements": ["select 1"]})),
    ] {
        let o = run_tool(&run, name, &input).await;
        assert!(!o.is_error, "{name}: {}", o.text);
    }
    let o = run_tool(
        &run,
        "done",
        &json!({"summary": "Set order 10 to 3\nbecause"}),
    )
    .await;
    assert!(!o.is_error, "{}", o.text);
    assert_eq!(run.state(), RunState::Submitted);
    fx.cleanup().await;
}

#[tokio::test]
async fn core_branches_history_snapshot() {
    let fx = fixture!("api_core");
    let g = fx.gnomon();
    assert!(g.status().await.unwrap().is_object());
    assert!(!g.doctor().await.unwrap().is_empty());

    let b = g
        .branches()
        .create("feature_x", Default::default())
        .await
        .unwrap();
    assert!(
        g.branches()
            .list()
            .await
            .unwrap()
            .iter()
            .any(|r| r["name"] == "feature_x")
    );
    let opts = g.branches().connection_options(&b, false).await.unwrap();
    assert!(!opts.is_empty());

    // write on the branch through a session put on it, then snapshot its diff
    let mut admin = fx.admin().await;
    sqlx::query("select gnomon.use('feature_x')")
        .execute(&mut admin)
        .await
        .unwrap();
    for s in [
        "insert into customers values (3, 'new@x.io')",
        "insert into orders (id, customer, total) values (12, 3, 5)",
        "update orders set total = 12 where id = 10",
    ] {
        sqlx::raw_sql(s).execute(&mut admin).await.unwrap();
    }
    sqlx::query("select gnomon.use('main')")
        .execute(&mut admin)
        .await
        .unwrap();
    let snap = gnomon_diff(&g, "feature_x", &[]).await.unwrap();
    assert_eq!(
        snap,
        "public.customers <customers:1> insert email = \"new@x.io\"\n\
         public.customers <customers:1> insert id = <customers:1>\n\
         public.orders <orders:1> update total: \"10.00\" -> \"12.00\"\n\
         public.orders <orders:2> insert big = 0\n\
         public.orders <orders:2> insert customer = <customers:1>\n\
         public.orders <orders:2> insert id = <orders:2>\n\
         public.orders <orders:2> insert total = \"5.00\"\n"
    );
    assert_changed(
        &g,
        "feature_x",
        "customers",
        &json!({"id": 3}),
        "email",
        &json!("new@x.io"),
    )
    .await
    .unwrap();
    assert!(
        assert_no_changes(&g, "feature_x", Some(&["customers"]))
            .await
            .is_err()
    );
    let blame = g
        .blame("customers", &json!({"id": 3}), Some("feature_x"))
        .await
        .unwrap();
    assert!(blame.iter().any(|b| b["col"] == "email"), "{blame:?}");
    g.branches().drop("feature_x").await.unwrap();

    with_scratch(&g, "t", |b| {
        let g = g.clone();
        async move {
            assert!(b.starts_with('t'), "{b}");
            assert_no_changes(&g, &b, None).await
        }
    })
    .await
    .unwrap();

    // a gnomon error carries its slug, and explain_error explains it
    let e = g
        .app_sql(
            "select gnomon.drop_branch('no_such_branch')",
            Default::default(),
        )
        .await
        .unwrap_err();
    assert!(e.code().is_some() || e.sqlstate().is_some(), "{e:?}");
    let why = g.explain_error(&e).await.unwrap();
    assert!(why.is_some_and(|w| !w.is_empty()));
    let plain = g
        .app_sql("select 1/0", Default::default())
        .await
        .unwrap_err();
    assert_eq!(g.explain_error(&plain).await.unwrap(), None);

    let rows = g
        .app_sql(
            "select 1::int4 as one, now() as at, '{1,2}'::int8[] as xs",
            Default::default(),
        )
        .await
        .unwrap();
    let r = row_to_json(&rows[0]).unwrap();
    assert_eq!(r["one"], 1);
    assert_eq!(r["xs"], json!([1, 2]));
    assert!(r["at"].is_string());
    let e = row_to_json(
        &g.app_sql("select '127.0.0.1'::inet as d", Default::default())
            .await
            .unwrap()[0],
    )
    .unwrap_err();
    assert!(e.message().contains("d::text"), "{e}");
    assert_eq!(rows[0].get::<i32, _>("one"), 1);
    fx.cleanup().await;
}
