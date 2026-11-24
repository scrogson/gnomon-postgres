//! What a dry run's statements change on top of the run's own changes.
//!
//! `gnomon.diff` on the run's branch is relative to main. A dry run reads it before and after its
//! statements, and the difference between the two, relative to the branch as it was, is what
//! `gnomon.dry_run` reports from its scratch branch.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::json::stable_json;
use crate::{Change, ChangeKind};

/// One row's change relative to main.
#[derive(Default)]
struct RowState {
    key: Value,
    insert: Option<Value>,
    delete: Option<Value>,
    /// column -> (main value, branch value)
    updates: BTreeMap<String, (Value, Value)>,
}

fn rows(diff: &[Change]) -> BTreeMap<(String, String), RowState> {
    let mut m: BTreeMap<(String, String), RowState> = BTreeMap::new();
    for c in diff {
        let r = m.entry((c.table.clone(), stable_json(&c.key))).or_default();
        r.key = c.key.clone();
        match c.kind {
            ChangeKind::Insert => r.insert = Some(c.new.clone()),
            ChangeKind::Delete => r.delete = Some(c.old.clone()),
            ChangeKind::Update => {
                if let Some(col) = &c.column {
                    r.updates
                        .insert(col.clone(), (c.old.clone(), c.new.clone()));
                }
            }
        }
    }
    m
}

fn same(a: Option<&RowState>, b: Option<&RowState>) -> bool {
    let f = |r: Option<&RowState>| {
        r.map(|r| {
            stable_json(&json!([
                r.insert,
                r.delete,
                r.updates
                    .iter()
                    .map(|(k, (o, n))| json!([k, o, n]))
                    .collect::<Vec<_>>()
            ]))
        })
    };
    f(a) == f(b)
}

fn image_with(base: &Value, updates: &BTreeMap<String, (Value, Value)>) -> Value {
    let mut m = base.as_object().cloned().unwrap_or_default();
    for (col, (_, new)) in updates {
        m.insert(col.clone(), new.clone());
    }
    Value::Object(m)
}

fn columns(a: &Value, b: &Value) -> BTreeSet<String> {
    let keys = |v: &Value| {
        v.as_object()
            .map(|m| m.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    };
    keys(a).into_iter().chain(keys(b)).collect()
}

/// The changes from the branch as `before` describes it to the branch as `after` does.
pub(crate) fn delta(before: &[Change], after: &[Change]) -> Vec<Change> {
    let b = rows(before);
    let a = rows(after);
    let keys: BTreeSet<&(String, String)> = b.keys().chain(a.keys()).collect();
    let mut out = Vec::new();
    for k in keys {
        let (rb, ra) = (b.get(k), a.get(k));
        if same(rb, ra) {
            continue;
        }
        let table = k.0.clone();
        let key = rb.or(ra).map(|r| r.key.clone()).unwrap_or(Value::Null);
        let change = |kind, column: Option<String>, old: Value, new: Value| Change {
            table: table.clone(),
            key: key.clone(),
            kind,
            column,
            old,
            new,
        };
        let upd = |c: &String, old: &Value, new: &Value| {
            change(
                ChangeKind::Update,
                Some(c.clone()),
                old.clone(),
                new.clone(),
            )
        };
        let empty = RowState::default();
        let rb = rb.unwrap_or(&empty);
        let ra = ra.unwrap_or(&empty);
        let untouched =
            |r: &RowState| r.insert.is_none() && r.delete.is_none() && r.updates.is_empty();
        match (rb, ra) {
            // new on top of the branch: as gnomon.diff has it
            (b, a) if untouched(b) => {
                if let Some(img) = &a.insert {
                    out.push(change(ChangeKind::Insert, None, Value::Null, img.clone()));
                }
                if let Some(img) = &a.delete {
                    out.push(change(ChangeKind::Delete, None, img.clone(), Value::Null));
                }
                for (c, (o, n)) in &a.updates {
                    out.push(upd(c, o, n));
                }
            }
            // a row the run inserted
            (
                RowState {
                    insert: Some(i1), ..
                },
                a,
            ) => match &a.insert {
                Some(i2) => {
                    for c in columns(i1, i2) {
                        let (o, n) = (&i1[c.as_str()], &i2[c.as_str()]);
                        if stable_json(o) != stable_json(n) {
                            out.push(upd(&c, o, n));
                        }
                    }
                }
                None => out.push(change(ChangeKind::Delete, None, i1.clone(), Value::Null)),
            },
            // a row the run deleted
            (
                RowState {
                    delete: Some(img), ..
                },
                a,
            ) => {
                if a.delete.is_none() {
                    out.push(change(
                        ChangeKind::Insert,
                        None,
                        Value::Null,
                        image_with(img, &a.updates),
                    ));
                }
            }
            // a row the run updated
            (b, a) => {
                if let Some(img) = &a.delete {
                    out.push(change(
                        ChangeKind::Delete,
                        None,
                        image_with(img, &b.updates),
                        Value::Null,
                    ));
                } else {
                    let cols: BTreeSet<&String> =
                        b.updates.keys().chain(a.updates.keys()).collect();
                    for c in cols {
                        let was = b
                            .updates
                            .get(c)
                            .map(|x| &x.1)
                            .or(a.updates.get(c).map(|x| &x.0));
                        let now = a
                            .updates
                            .get(c)
                            .map(|x| &x.1)
                            .or(b.updates.get(c).map(|x| &x.0));
                        if let (Some(o), Some(n)) = (was, now)
                            && stable_json(o) != stable_json(n)
                        {
                            out.push(upd(c, o, n));
                        }
                    }
                }
            }
        }
    }
    out
}

/// Tables in the order `tables` first lists them, then keys ascending (numbers numerically).
pub(crate) fn sort<'a>(diff: &mut [Change], tables: impl Iterator<Item = &'a str>) {
    let mut rank: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for t in tables {
        let n = rank.len();
        rank.entry(t.to_string()).or_insert(n);
    }
    diff.sort_by(|a, b| {
        let r = |t: &str| rank.get(t).copied().unwrap_or(usize::MAX);
        r(&a.table)
            .cmp(&r(&b.table))
            .then_with(|| crate::testing::cmp_key(&a.key, &b.key))
    });
}

/// Rows inserted, updated and deleted per table, as `gnomon.diff_summary` counts them.
pub(crate) fn summary(diff: &[Change]) -> Value {
    let mut t: BTreeMap<&str, [BTreeSet<String>; 3]> = BTreeMap::new();
    for c in diff {
        let i = match c.kind {
            ChangeKind::Insert => 0,
            ChangeKind::Update => 1,
            ChangeKind::Delete => 2,
        };
        t.entry(&c.table).or_default()[i].insert(stable_json(&c.key));
    }
    Value::Array(
        t.into_iter()
            .map(|(table, [i, u, d])| {
                json!({ "table": table, "inserts": i.len(), "updates": u.len(), "deletes": d.len() })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(kind: ChangeKind, id: i64, col: Option<&str>, old: Value, new: Value) -> Change {
        Change {
            table: "public.t".into(),
            key: json!({ "id": id }),
            kind,
            column: col.map(str::to_string),
            old,
            new,
        }
    }
    use ChangeKind::*;

    #[test]
    fn deltas() {
        let before = vec![
            ch(Update, 1, Some("v"), json!(1), json!(9)),
            ch(Insert, 5, None, Value::Null, json!({"id": 5, "v": 5})),
            ch(Delete, 3, None, json!({"id": 3, "v": 3}), Value::Null),
            ch(Update, 4, Some("v"), json!(4), json!(40)),
        ];
        let after = vec![
            // 1: changed again, old must be the run's value 9
            ch(Update, 1, Some("v"), json!(1), json!(7)),
            // 2: fresh change
            ch(Update, 2, Some("v"), json!(2), json!(3)),
            // 5: the run's insert, changed
            ch(Insert, 5, None, Value::Null, json!({"id": 5, "v": 6})),
            // 3: deleted row put back as in main; 4: run's update undone
        ];
        let d = delta(&before, &after);
        let lines: Vec<String> = d
            .iter()
            .map(|c| format!("{:?} {} {:?} {} {}", c.kind, c.key, c.column, c.old, c.new))
            .collect();
        assert_eq!(
            lines,
            [
                r#"Update {"id":1} Some("v") 9 7"#,
                r#"Update {"id":2} Some("v") 2 3"#,
                r#"Insert {"id":3} None null {"id":3,"v":3}"#,
                r#"Update {"id":4} Some("v") 40 4"#,
                r#"Update {"id":5} Some("v") 5 6"#,
            ]
        );
        assert_eq!(
            summary(&d),
            json!([{ "table": "public.t", "inserts": 1, "updates": 4, "deletes": 0 }])
        );
        assert!(delta(&before, &before).is_empty());
        // the run's insert deleted; an updated row deleted (old is the branch's row)
        let after = vec![
            ch(Update, 1, Some("v"), json!(1), json!(9)),
            ch(Delete, 3, None, json!({"id": 3, "v": 3}), Value::Null),
            ch(Delete, 4, None, json!({"id": 4, "v": 4}), Value::Null),
        ];
        let d = delta(&before, &after);
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].kind, Delete);
        assert_eq!(d[0].old, json!({"id": 4, "v": 40}));
        assert_eq!(d[1].old, json!({"id": 5, "v": 5}));
    }
}
