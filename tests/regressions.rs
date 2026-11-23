//! Regressions found by review: each test is one reported failure.

mod common;

use std::time::Duration;

use gnomon::tools::run_tool;
use gnomon::{RunState, SqlOptions, StartRun, row_to_json, row_to_json_lossy};
use serde_json::json;
use sqlx::AssertSqlSafe;

fn setup() -> Vec<String> {
    [
        "create type st as enum ('new', 'done')",
        "create table t (id int primary key, v int not null, s st not null default 'new', d interval not null default '1 day')",
        "insert into t values (1, 1, 'new', '1 day')",
        "select gnomon.track('t')",
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

/// The sql tool committed a write, then told the model it failed over an enum/interval column.
#[tokio::test]
async fn sql_tool_never_reports_a_committed_write_as_failed() {
    let fx = fixture!("reg_tool_types");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("types")).await.unwrap();
    let o = run_tool(
        &run,
        "sql",
        &json!({"statement": "update t set v = 42 where id = 1 returning id, s, d"}),
    )
    .await;
    assert!(!o.is_error, "{}", o.text);
    let v = o.value.unwrap();
    assert_eq!(v["rows"][0]["s"], "new");
    assert_eq!(v["rows"][0]["d"], "1 day");
    // a type still not converted is a placeholder, not an error
    let o = run_tool(
        &run,
        "sql",
        &json!({"statement": "select '127.0.0.1'::inet as ip"}),
    )
    .await;
    assert!(!o.is_error, "{}", o.text);
    assert_eq!(
        o.value.unwrap()["rows"][0]["ip"],
        "<inet: cast to ::text to read it>"
    );
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// Values compared with PostgreSQL's own text output.
#[tokio::test]
async fn converted_values_match_postgres_text() {
    let fx = fixture!("reg_values");
    let g = fx.gnomon();
    for (expr, want) in [
        ("'0.1'::float4", json!(0.1)),
        ("'{0.1,null}'::float4[]", json!([0.1, null])),
        ("'done'::st", json!("done")),
        (
            "interval '1 year 2 mons 3 days 04:05:06.5'",
            json!("1 year 2 mons 3 days 04:05:06.5"),
        ),
        ("interval '-1 mons -01:00:00'", json!("-1 mons -01:00:00")),
        ("interval '0'", json!("00:00:00")),
        ("'{2026-01-02,null}'::date[]", json!(["2026-01-02", null])),
        ("'infinity'::interval", json!("infinity")),
        ("'-9.313642145546353'::numeric", json!(-9.313642145546353)),
        ("'3106.0529166137962'::float8", json!(3106.0529166137962)),
        ("'1e300'::float8", json!(1e300)),
        (
            r#"'{"a": 1.10, "b": 123456789012345678901234567890, "c": -9.313642145546353}'::jsonb"#,
            json!({"a": "1.10", "b": "123456789012345678901234567890", "c": -9.313642145546353}),
        ),
        (r#"'{"a": 1.10}'::json"#, json!({"a": "1.10"})),
        (
            r#"array['{"a": 1.10}'::jsonb, null]"#,
            json!([{"a": "1.10"}, null]),
        ),
    ] {
        let sql = format!("select {expr} as x");
        let rows = g.app_sql(&sql, Default::default()).await.unwrap();
        let got = row_to_json(&rows[0]).unwrap_or_else(|e| panic!("{expr}: {e}"));
        assert_eq!(got["x"].to_string(), want.to_string(), "{expr}");
    }
    fx.cleanup().await;
}

/// jsonb nested past serde_json's 128 failed; past the crate's cap it is an error, never a crash.
#[tokio::test]
async fn deep_json_converts_up_to_the_cap() {
    let fx = fixture!("reg_deep_json");
    let g = fx.gnomon();
    let deep =
        |n: usize| format!("select (repeat('[', {n}) || '1.10' || repeat(']', {n}))::jsonb as x");
    for n in [200, 400] {
        let rows = g.app_sql(&deep(n), Default::default()).await.unwrap();
        let got = row_to_json(&rows[0]).unwrap_or_else(|e| panic!("{n}: {e}"));
        assert!(got["x"].to_string().contains("\"1.10\""), "{n}");
    }
    let rows = g.app_sql(&deep(401), Default::default()).await.unwrap();
    let e = row_to_json(&rows[0]).unwrap_err();
    assert!(e.to_string().contains("::text"), "{e}");
    assert!(row_to_json_lossy(&rows[0])["x"].is_string());
    let rows = g
        .app_sql("select '[-0]'::json as x", Default::default())
        .await
        .unwrap();
    assert_eq!(row_to_json(&rows[0]).unwrap()["x"], json!(["-0"]));
    fx.cleanup().await;
}

/// merge_opts(false) left the run Open, so with_run then discarded a merged branch.
#[tokio::test]
async fn merge_without_close_is_still_merged() {
    let fx = fixture!("reg_merge_open");
    let g = fx.gnomon();
    let mut branch = String::new();
    g.with_run(StartRun::new().task("keep open"), |run| {
        branch = run.branch().to_string();
        async move {
            run.sql("update t set v = 7 where id = 1").await?;
            run.merge_opts(false).await?;
            assert_eq!(run.state(), RunState::Merged);
            Ok(())
        }
    })
    .await
    .unwrap();
    let mut admin = fx.admin().await;
    let status: String = sqlx::query_scalar("select status from gnomon.branches where name = $1")
        .bind(&branch)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_ne!(
        status, "closed",
        "with_run must not end a branch the closure merged"
    );
    fx.cleanup().await;
}

/// A sub-millisecond timeout became statement_timeout = 0, which is no limit.
#[tokio::test]
async fn tiny_timeouts_still_limit() {
    let fx = fixture!("reg_timeout");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("timeout")).await.unwrap();
    let e = run
        .sql_opts(
            "select pg_sleep(0.3)",
            Default::default(),
            SqlOptions {
                timeout: Some(Duration::from_micros(500)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind().class_name(), "StatementTimeoutError", "{e}");
    run.discard().await.unwrap();
    fx.cleanup().await;
}

/// A user in the base URL's query made Run::connect log in as that user.
#[tokio::test]
async fn run_connection_is_the_runs_own_login() {
    let fx = fixture!("reg_url_user");
    let g = fx.gnomon();
    let run = g.start_run(StartRun::new().task("url")).await.unwrap();
    let mut base = url::Url::parse(&fx.server).unwrap();
    base.set_path(&fx.db);
    base.set_username("").unwrap();
    base.set_query(Some("user=postgres&sslmode=disable"));
    let mut conn = run.connect(Some(base.as_str())).await.unwrap();
    let who: String = sqlx::query_scalar(AssertSqlSafe("select current_user::text"))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_ne!(who, "postgres");
    assert!(run.connect(Some("host=127.0.0.1 dbname=x")).await.is_err());
    drop(conn);
    run.discard().await.unwrap();
    fx.cleanup().await;
}
