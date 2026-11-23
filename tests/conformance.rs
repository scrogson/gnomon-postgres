//! The conformance scenarios of the specification (`spec/conformance/*.json`), run as every gnomon
//! SDK runs them. A step marked `sqlOnly` is skipped when the server loads the C module.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use gnomon::{Run, SqlOptions, StartRun, row_to_json};
use serde_json::{Value, json};

fn get<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(v, |x, k| x.get(k))
}

async fn scenario(server: &str, file: &str) {
    let text = std::fs::read_to_string(format!(
        "{}/spec/conformance/{file}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let sc: Value = serde_json::from_str(&text).unwrap();
    let setup: Vec<String> = serde_json::from_value(sc["setup"].clone()).unwrap();
    let fx = common::Fixture::new(
        server,
        &format!("conf_{}", file.trim_end_matches(".json")),
        &setup,
    )
    .await;
    let g = fx.gnomon();
    let mut runs: HashMap<String, Run> = HashMap::new();
    for (i, st) in sc["steps"].as_array().unwrap().iter().enumerate() {
        let call = st["call"].as_str().unwrap();
        let at = format!("{file} step {} ({call})", i + 1);
        let exp = st.get("expect").cloned().unwrap_or(json!({}));
        if exp.get("sqlOnly") == Some(&json!(true)) && fx.cmod {
            eprintln!("{at}: skipped (sqlOnly, the server loads the C module)");
            continue;
        }
        let on = st.get("on").and_then(Value::as_str).unwrap_or("default");
        let run = || runs.get(on).unwrap_or_else(|| panic!("{at}: no run {on}"));
        let statement = st["args"]["statement"].as_str().unwrap_or("");
        let out: gnomon::Result<Value> = match call {
            "startRun" => {
                let a = &st["args"];
                let mut o = StartRun::new();
                if let Some(t) = a["task"].as_str() {
                    o = o.task(t);
                }
                if let Some(t) = a["actor"].as_str() {
                    o = o.actor(t);
                }
                let r = g.start_run(o).await.unwrap_or_else(|e| panic!("{at}: {e}"));
                runs.insert(
                    st.get("as")
                        .and_then(Value::as_str)
                        .unwrap_or("default")
                        .to_string(),
                    r,
                );
                continue;
            }
            "run.sql" => {
                let timeout = st["options"]["timeoutMs"]
                    .as_u64()
                    .map(Duration::from_millis);
                let opts = SqlOptions {
                    timeout,
                    ..Default::default()
                };
                run()
                    .sql_opts(statement, Default::default(), opts)
                    .await
                    .and_then(|r| Ok(json!({ "rowCount": r.row_count, "rows": r.json_rows()? })))
            }
            "app.sql" => g
                .app_sql(statement, Default::default())
                .await
                .and_then(|rows| {
                    let rows = rows
                        .iter()
                        .map(row_to_json)
                        .collect::<gnomon::Result<Vec<_>>>()?;
                    Ok(json!({ "rowCount": rows.len(), "rows": rows }))
                }),
            "run.diff" => run().diff().await.map(|d| serde_json::to_value(d).unwrap()),
            "run.summary" => run()
                .summary()
                .await
                .map(|s| serde_json::to_value(s).unwrap()),
            "run.merge" => run()
                .merge()
                .await
                .map(|m| serde_json::to_value(m).unwrap()),
            "run.discard" => run().discard().await.map(|_| Value::Null),
            other => panic!("{at}: unknown call {other}"),
        };
        match (out, exp.get("error").and_then(Value::as_str)) {
            (Err(e), Some(want)) => {
                // GnomonError is the base class: any gnomon error matches it
                assert!(
                    want == "GnomonError" || e.kind().class_name() == want,
                    "{at}: expected {want}, got {e}"
                );
            }
            (Err(e), None) => panic!("{at}: {e}"),
            (Ok(v), Some(want)) => panic!("{at}: expected {want}, got {v}"),
            (Ok(v), None) => {
                let exp = exp.as_object().unwrap();
                if let Some(n) = exp.get("rowCount") {
                    assert_eq!(&v["rowCount"], n, "{at}");
                }
                if let Some(rows) = exp.get("rows") {
                    assert_eq!(&v["rows"], rows, "{at}");
                }
                if let Some(n) = exp.get("length") {
                    assert_eq!(
                        v.as_array().map(Vec::len),
                        n.as_u64().map(|n| n as usize),
                        "{at}"
                    );
                }
                for (k, want) in exp.iter().filter(|(k, _)| k.contains('.')) {
                    assert_eq!(get(&v, k), Some(want), "{at}: {k}");
                }
            }
        }
    }
    fx.cleanup().await;
}

#[tokio::test]
async fn conformance() {
    let Some(server) = common::server() else {
        return;
    };
    let mut files: Vec<String> =
        std::fs::read_dir(format!("{}/spec/conformance", env!("CARGO_MANIFEST_DIR")))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|f| f.ends_with(".json"))
            .collect();
    files.sort();
    assert!(!files.is_empty());
    for f in files {
        scenario(&server, &f).await;
        eprintln!("{f}: ok");
    }
}
