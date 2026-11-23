//! The testing layer: a branch per test, and a diff normalized for a stored snapshot
//! (spec: Snapshots). Same rules and golden files as the TypeScript and Python SDKs.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::json::stable_json;
use crate::{Change, ChangeKind, Gnomon};

/// A single foreign key, as `gnomon.describe` reports it (table names schema-qualified).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub columns: Vec<String>,
    pub references: String,
    pub referenced_columns: Vec<String>,
}

fn cmp_scalar(x: Option<&Value>, y: Option<&Value>) -> Ordering {
    match (x, y) {
        (Some(Value::Number(a)), Some(Value::Number(b))) => {
            // integers of any size compare by value; others as floats
            match (a.as_str_int(), b.as_str_int()) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => a
                    .as_f64()
                    .unwrap_or(0.0)
                    .partial_cmp(&b.as_f64().unwrap_or(0.0))
                    .unwrap_or(Ordering::Equal),
            }
        }
        (a, b) => text(a).cmp(&text(b)),
    }
}

trait IntText {
    fn as_str_int(&self) -> Option<i128>;
}
impl IntText for serde_json::Number {
    fn as_str_int(&self) -> Option<i128> {
        self.to_string().parse::<i128>().ok()
    }
}

fn text(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(Value::String(s)) => s.clone(),
        Some(x) => x.to_string(),
    }
}

/// Ascending key order: column by column, numbers numerically, text by code point.
fn cmp_key(a: &Value, b: &Value) -> Ordering {
    let empty = Map::new();
    let ao = a.as_object().unwrap_or(&empty);
    let bo = b.as_object().unwrap_or(&empty);
    let mut cols: Vec<&String> = ao.keys().collect();
    cols.sort();
    for c in cols {
        let o = cmp_scalar(ao.get(c), bo.get(c));
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

fn short(t: &str) -> &str {
    t.rsplit('.').next().unwrap_or(t)
}

/// Normalize a diff for a snapshot: placeholders for keys, foreign keys followed, `ignore`d
/// columns dropped. One line per changed field, sorted by table, placeholder, then text.
pub fn normalize_diff(
    diff: &[Change],
    ignore: &[&str],
    foreign_keys: &[ForeignKey],
) -> Vec<String> {
    let ignore: HashSet<&str> = ignore.iter().copied().collect();
    // placeholders per table, in ascending key order
    let mut by_table: BTreeMap<&str, Vec<&Value>> = BTreeMap::new();
    for c in diff {
        let ks = by_table.entry(&c.table).or_default();
        if !ks.iter().any(|k| stable_json(k) == stable_json(&c.key)) {
            ks.push(&c.key);
        }
    }
    let mut ph: HashMap<String, (String, usize)> = HashMap::new();
    for (t, ks) in by_table.iter_mut() {
        ks.sort_by(|a, b| cmp_key(a, b));
        for (i, k) in ks.iter().enumerate() {
            ph.insert(
                format!("{t}|{}", stable_json(k)),
                (format!("<{}:{}>", short(t), i + 1), i + 1),
            );
        }
    }
    let value = |table: &str, key: &Value, col: &str, v: &Value| -> String {
        if let Some(ko) = key.as_object()
            && ko.len() == 1
            && ko.contains_key(col)
            && let Some((p, _)) = ph.get(&format!("{table}|{}", stable_json(key)))
        {
            return p.clone();
        }
        for f in foreign_keys {
            if f.table == table && f.columns.len() == 1 && f.columns[0] == col {
                let mut k = Map::new();
                k.insert(f.referenced_columns[0].clone(), v.clone());
                if let Some((p, _)) = ph.get(&format!(
                    "{}|{}",
                    f.references,
                    stable_json(&Value::Object(k))
                )) {
                    return p.clone();
                }
            }
        }
        stable_json(v)
    };
    let mut lines: Vec<(String, usize, String)> = Vec::new();
    for c in diff {
        let (p, i) = ph[&format!("{}|{}", c.table, stable_json(&c.key))].clone();
        let t = &c.table;
        match c.kind {
            ChangeKind::Delete => lines.push((t.clone(), i, format!("{t} {p} delete"))),
            ChangeKind::Insert => {
                if let Some(img) = c.new.as_object() {
                    for (col, v) in img {
                        if ignore.contains(col.as_str()) {
                            continue;
                        }
                        lines.push((
                            t.clone(),
                            i,
                            format!("{t} {p} insert {col} = {}", value(t, &c.key, col, v)),
                        ));
                    }
                }
            }
            ChangeKind::Update => {
                let Some(col) = c.column.as_deref() else {
                    continue;
                };
                if ignore.contains(col) {
                    continue;
                }
                lines.push((
                    t.clone(),
                    i,
                    format!(
                        "{t} {p} update {col}: {} -> {}",
                        value(t, &c.key, col, &c.old),
                        value(t, &c.key, col, &c.new)
                    ),
                ));
            }
        }
    }
    let mut seen = HashSet::new();
    lines.retain(|l| seen.insert(l.2.clone()));
    lines.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    lines.into_iter().map(|l| l.2).collect()
}

/// A branch's diff as snapshot text (for `insta` or a stored file), foreign keys taken from
/// `gnomon.describe`.
pub async fn gnomon_diff(g: &Gnomon, branch: &str, ignore: &[&str]) -> Result<String> {
    let mut diff = g.diff(branch, None).await?;
    let describe = g.describe(None).await?;
    let mut fks = Vec::new();
    for t in describe
        .get("tables")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let schema = t.get("schema").and_then(Value::as_str).unwrap_or("public");
        let name = t.get("name").and_then(Value::as_str).unwrap_or("");
        for f in t
            .get("foreign_keys")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let strs = |k: &str| -> Vec<String> {
                f.get(k)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            };
            let r = f.get("references").and_then(Value::as_str).unwrap_or("");
            fks.push(ForeignKey {
                table: format!("{schema}.{name}"),
                columns: strs("columns"),
                references: if r.contains('.') {
                    r.to_string()
                } else {
                    format!("{schema}.{r}")
                },
                referenced_columns: strs("referenced_columns"),
            });
        }
    }
    for c in &mut diff {
        if !c.table.contains('.') {
            c.table = format!("public.{}", c.table);
        }
    }
    Ok(normalize_diff(&diff, ignore, &fks).join("\n") + "\n")
}

fn table_matches(t: &str, want: &str) -> bool {
    t == want || t.ends_with(&format!(".{want}"))
}

/// Nothing changed on the branch (in these tables, if given).
pub async fn assert_no_changes(g: &Gnomon, branch: &str, tables: Option<&[&str]>) -> Result<()> {
    let d = g.diff(branch, None).await?;
    let hit: Vec<&Change> = d
        .iter()
        .filter(|c| tables.is_none_or(|ts| ts.iter().any(|t| table_matches(&c.table, t))))
        .collect();
    if !hit.is_empty() {
        let first = serde_json::to_value(&hit[..hit.len().min(5)])?;
        return Err(Error::other(format!(
            "expected no changes, found {}: {}",
            hit.len(),
            stable_json(&first)
        )));
    }
    Ok(())
}

/// One field of one row changed to a value on the branch (by an update or an insert).
pub async fn assert_changed(
    g: &Gnomon,
    branch: &str,
    table: &str,
    key: &Value,
    column: &str,
    value: &Value,
) -> Result<()> {
    let d = g.diff(branch, None).await?;
    let want = stable_json(value);
    let ok = d.iter().any(|c| {
        table_matches(&c.table, table)
            && stable_json(&c.key) == stable_json(key)
            && match c.kind {
                ChangeKind::Update => {
                    c.column.as_deref() == Some(column) && stable_json(&c.new) == want
                }
                ChangeKind::Insert => c.new.get(column).is_some_and(|v| stable_json(v) == want),
                ChangeKind::Delete => false,
            }
    });
    if !ok {
        return Err(Error::other(format!(
            "expected {table} {}.{column} = {want} on {branch}",
            stable_json(key)
        )));
    }
    Ok(())
}

/// A scratch branch for one test: created, handed to `f`, then dropped.
pub async fn with_scratch<F, Fut, T>(g: &Gnomon, prefix: &str, f: F) -> Result<T>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let b = g.branches().scratch(prefix, "main").await?;
    let out = f(b.clone()).await;
    let _ = g.branches().drop(&b).await;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Golden {
        ignore: Vec<String>,
        foreign_keys: Vec<ForeignKey>,
        diff: Vec<Value>,
        expected: Vec<String>,
    }

    #[test]
    fn golden_snapshot() {
        let g: Golden = serde_json::from_str(include_str!("../spec/snapshot/basic.json")).unwrap();
        let diff: Vec<Change> = g
            .diff
            .into_iter()
            .map(|mut d| {
                let col = d["col"].take();
                d.as_object_mut().unwrap().insert("column".into(), col);
                serde_json::from_value(d).unwrap()
            })
            .collect();
        let ignore: Vec<&str> = g.ignore.iter().map(String::as_str).collect();
        assert_eq!(normalize_diff(&diff, &ignore, &g.foreign_keys), g.expected);
    }
}
