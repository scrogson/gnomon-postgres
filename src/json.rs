//! Values (spec: Values). jsonb comes back as text and is parsed losslessly: an integer that fits
//! `i64`/`u64` is a JSON number, and so is a decimal whose text an `f64` prints back unchanged;
//! anything else (`1.10`, `0.30000000000000000001`, an integer past 2^64) is kept as a string of
//! the database's exact text. This is the specification's rule, without serde_json's
//! `arbitrary_precision` (which would change serde_json for every crate in your build).
//! Rows of a run's statements convert to JSON here, for tools and for callers that want maps.

use serde_json::{Map, Number, Value};
use sqlx::postgres::types::{Oid, PgInterval};
use sqlx::postgres::{PgHasArrayType, PgRow, PgTypeInfo, PgTypeKind, PgValueFormat, PgValueRef};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use crate::error::{Error, Result};

/// Parse jsonb text losslessly; SQL NULL is JSON null.
pub fn parse_json(text: Option<&str>) -> Result<Value> {
    match text {
        None => Ok(Value::Null),
        Some(t) => {
            let (quoted, depth) = quote_inexact_numbers(t);
            if depth > MAX_DEPTH {
                return Err(Error::other(format!(
                    "JSON nested {depth} levels deep (more than {MAX_DEPTH}): cast it to ::text to read it"
                )));
            }
            Ok(from_str_deep(&quoted)?)
        }
    }
}

/// [`parse_json`], except that a container nested past the cap becomes a string saying so,
/// instead of failing the whole value. For diffs: a row with one deep column still shows the
/// rest.
pub(crate) fn parse_json_capped(text: Option<&str>) -> Result<Value> {
    match text {
        None => Ok(Value::Null),
        Some(t) => {
            let (quoted, depth) = quote_inexact_numbers(t);
            if depth > MAX_DEPTH {
                return Ok(from_str_deep(&cut_deeper_than(&quoted, MAX_DEPTH))?);
            }
            Ok(from_str_deep(&quoted)?)
        }
    }
}

/// What stands in for a container nested past [`MAX_DEPTH`]: a string naming a hash of the text
/// it replaces, so two different deep values never look the same in a diff.
fn too_deep(cut: &str) -> String {
    // FNV-1a: stable across builds and platforms, unlike std's hasher
    let mut h: u64 = 0xcbf29ce484222325;
    for b in cut.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
    format!(
        "\"<JSON nested more than {MAX_DEPTH} levels deep (#{h:016x}): cast it to ::text to read it>\""
    )
}

/// Valid JSON text with every container that would open past `max` levels replaced by
/// [`too_deep`].
fn cut_deeper_than(s: &str, max: usize) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let (mut depth, mut i, mut from) = (0usize, 0usize, 0usize);
    let mut in_str = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            match c {
                b'\\' => i += 1,
                b'"' => in_str = false,
                _ => {}
            }
        } else {
            match c {
                b'"' => in_str = true,
                b'[' | b'{' if depth == max => {
                    out.push_str(&s[from..i]);
                    let start = i;
                    // skip to the matching close
                    let (mut d, mut q) = (0usize, false);
                    while i < b.len() {
                        let c = b[i];
                        if q {
                            match c {
                                b'\\' => i += 1,
                                b'"' => q = false,
                                _ => {}
                            }
                        } else {
                            match c {
                                b'"' => q = true,
                                b'[' | b'{' => d += 1,
                                b']' | b'}' => {
                                    d -= 1;
                                    if d == 0 {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        i += 1;
                    }
                    out.push_str(&too_deep(&s[start..(i + 1).min(s.len())]));
                    from = i + 1;
                }
                b'[' | b'{' => depth += 1,
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        i += 1;
    }
    out.push_str(&s[from.min(s.len())..]);
    out
}

/// The deepest nesting parsed. serde_json stops at 128; past this, recursive code anywhere
/// (drop, Display, a caller's own) risks overflowing a 2 MiB thread stack.
const MAX_DEPTH: usize = 400;

/// serde_json without its depth limit of 128 (PostgreSQL nests deeper), on a growing stack.
fn from_str_deep(s: &str) -> serde_json::Result<Value> {
    use serde::Deserialize;
    let mut de = serde_json::Deserializer::from_str(s);
    de.disable_recursion_limit();
    let v = Value::deserialize(serde_stacker::Deserializer::new(&mut de))?;
    de.end()?;
    Ok(v)
}

/// Whether a token is a number by JSON's grammar: `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?`.
fn json_number(t: &str) -> bool {
    let b = t.as_bytes();
    let mut i = usize::from(b.first() == Some(&b'-'));
    let digits = |i: &mut usize| {
        let s = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > s
    };
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => {
            digits(&mut i);
        }
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == b.len()
}

/// Whether a JSON number token survives as a serde_json number: an integer within i64/u64, or a
/// decimal an f64 prints back to the same text.
fn exact(token: &str) -> bool {
    // serde_json reads -0 as the float -0.0: keep its text
    if token == "-0" {
        return false;
    }
    if token.parse::<i64>().is_ok() || token.parse::<u64>().is_ok() {
        return true;
    }
    let integer = !token.contains(['.', 'e', 'E']);
    !integer
        && token
            .parse::<f64>()
            .is_ok_and(|f| f.is_finite() && f.to_string() == token)
}

/// JSON text with every number token that would not survive (see [`exact`]) put in quotes.
/// Also returns the deepest nesting of arrays and objects.
fn quote_inexact_numbers(t: &str) -> (std::borrow::Cow<'_, str>, usize) {
    let b = t.as_bytes();
    let (mut depth, mut max) = (0usize, 0usize);
    let mut out: Option<String> = None;
    let mut last = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < b.len() && matches!(b[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                let tok = &t[start..i];
                // a malformed token stays as it is, so the parse rejects it
                // nor does a number in key position (`{1.10: ..}`), which is invalid JSON too
                let key = t[i..].trim_start().starts_with(':');
                if json_number(tok) && !exact(tok) && !key {
                    let o = out.get_or_insert_with(|| String::with_capacity(t.len() + 8));
                    o.push_str(&t[last..start]);
                    o.push('"');
                    o.push_str(tok);
                    o.push('"');
                    last = i;
                }
            }
            b'[' | b'{' => {
                depth += 1;
                max = max.max(depth);
                i += 1;
            }
            b']' | b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => i += 1,
        }
    }
    let text = match out {
        None => std::borrow::Cow::Borrowed(t),
        Some(mut o) => {
            o.push_str(&t[last..]);
            std::borrow::Cow::Owned(o)
        }
    };
    (text, max)
}

/// JSON text with object keys sorted (snapshots, digests, tool output).
pub fn stable_json(v: &Value) -> String {
    serde_json::to_string(&sort_keys(v)).expect("a serde_json::Value always serializes")
}

fn sort_keys(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(sort_keys).collect()),
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let mut m = Map::new();
            for k in keys {
                m.insert(k.clone(), sort_keys(&o[k]));
            }
            Value::Object(m)
        }
        x => x.clone(),
    }
}

/// A row as a JSON object, column name to value.
///
/// Covers the common PostgreSQL types: booleans, integers, floats, numeric, text types, enums,
/// json, uuid, dates, times and intervals, bytea, and one-dimensional arrays of the common scalars.
/// Any other type is an error that names the column; cast it to `::text` in the statement, or use
/// [`row_to_json_lossy`].
pub fn row_to_json(row: &PgRow) -> Result<Map<String, Value>> {
    let mut m = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(i)?;
        let v = if raw.is_null() {
            Value::Null
        } else {
            value_to_json(row, i, &raw)?
        };
        m.insert(col.name().to_string(), v);
    }
    Ok(m)
}

/// Like [`row_to_json`], but a value of a type it does not convert becomes a string saying so
/// (`"<interval: cast to ::text to read it>"`) instead of failing the whole row. Tools use this:
/// a statement that committed must never come back to a model as an error.
pub fn row_to_json_lossy(row: &PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let v = match row.try_get_raw(i) {
            Ok(raw) if raw.is_null() => Value::Null,
            Ok(raw) => value_to_json(row, i, &raw).unwrap_or_else(|_| {
                Value::String(format!(
                    "<{}: cast to ::text to read it>",
                    raw.type_info().name().to_lowercase()
                ))
            }),
            Err(e) => Value::String(format!("<unreadable: {e}>")),
        };
        m.insert(col.name().to_string(), v);
    }
    m
}

/// An interval as PostgreSQL prints it (`IntervalStyle = postgres`): `1 year 2 mons 3 days 04:05:06.5`.
fn interval_text(iv: &PgInterval) -> String {
    // as PostgreSQL's EncodeInterval: a positive field after a negative one carries an explicit +
    match (iv.months, iv.days, iv.microseconds) {
        (i32::MAX, i32::MAX, i64::MAX) => return "infinity".into(),
        (i32::MIN, i32::MIN, i64::MIN) => return "-infinity".into(),
        _ => {}
    }
    let mut parts: Vec<String> = Vec::new();
    let mut before_negative = false;
    let (y, mo) = (iv.months / 12, iv.months % 12);
    for (n, one, many) in [
        (y as i64, "year", "years"),
        (mo as i64, "mon", "mons"),
        (iv.days as i64, "day", "days"),
    ] {
        if n == 0 {
            continue;
        }
        let plus = if before_negative && n > 0 { "+" } else { "" };
        parts.push(format!("{plus}{n} {}", if n == 1 { one } else { many }));
        before_negative = n < 0;
    }
    if iv.microseconds != 0 || parts.is_empty() {
        let neg = iv.microseconds < 0;
        let us = iv.microseconds.unsigned_abs();
        let (h, m, sec, frac) = (
            us / 3_600_000_000,
            us / 60_000_000 % 60,
            us / 1_000_000 % 60,
            us % 1_000_000,
        );
        let sign = if neg {
            "-"
        } else if before_negative {
            "+"
        } else {
            ""
        };
        let mut t = format!("{sign}{h:02}:{m:02}:{sec:02}");
        if frac != 0 {
            t.push_str(format!(".{frac:06}").trim_end_matches('0'));
        }
        parts.push(t);
    }
    parts.join(" ")
}

fn num(text: &str) -> Value {
    // a number when it survives exactly; otherwise the database's text (spec: Values)
    match serde_json::from_str::<Number>(text) {
        Ok(n) if exact(text) => Value::Number(n),
        _ => Value::String(text.to_string()),
    }
}

/// A float column is always a JSON number (an f64 is exact as itself); NaN and the infinities,
/// which JSON cannot hold, are PostgreSQL's text.
fn float(f: f64) -> Value {
    match serde_json::Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None if f.is_nan() => Value::String("NaN".into()),
        None if f > 0.0 => Value::String("Infinity".into()),
        None => Value::String("-Infinity".into()),
    }
}

fn float32(f: f32) -> Value {
    // f32's own shortest form, read as f64: 0.1, not 0.10000000149011612
    float(f.to_string().parse::<f64>().unwrap_or(f as f64))
}

/// A json or jsonb value as its text (jsonb's binary form is a version byte, then the text),
/// parsed by [`parse_json`] so its numbers follow the same rule as everywhere else.
struct JsonText(String);

impl sqlx::Type<sqlx::Postgres> for JsonText {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(3802))
    }
    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(ty.name(), "JSON" | "JSONB")
    }
}

impl PgHasArrayType for JsonText {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(3807))
    }
    fn array_compatible(ty: &PgTypeInfo) -> bool {
        matches!(ty.name(), "JSON[]" | "JSONB[]")
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JsonText {
    fn decode(value: PgValueRef<'r>) -> std::result::Result<Self, sqlx::error::BoxDynError> {
        let jsonb = value.type_info().name() == "JSONB";
        let mut b = value.as_bytes()?;
        if jsonb && value.format() == PgValueFormat::Binary {
            match b.split_first() {
                Some((1, rest)) => b = rest,
                _ => return Err("unsupported jsonb binary version".into()),
            }
        }
        Ok(JsonText(std::str::from_utf8(b)?.to_string()))
    }
}

fn json_text(j: JsonText) -> Result<Value> {
    parse_json(Some(&j.0))
}

fn arr<T>(v: Vec<Option<T>>, f: impl Fn(T) -> Value) -> Value {
    Value::Array(
        v.into_iter()
            .map(|x| x.map(&f).unwrap_or(Value::Null))
            .collect(),
    )
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

fn value_to_json(row: &PgRow, i: usize, raw: &PgValueRef<'_>) -> Result<Value> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, Utc};
    let ty = raw.type_info().name().to_string();
    Ok(match ty.as_str() {
        "VOID" => Value::Null,
        "BOOL" => Value::Bool(row.try_get(i)?),
        "INT2" => Value::from(row.try_get::<i16, _>(i)?),
        "INT4" => Value::from(row.try_get::<i32, _>(i)?),
        "INT8" => Value::from(row.try_get::<i64, _>(i)?),
        "OID" => Value::from(row.try_get::<sqlx::postgres::types::Oid, _>(i)?.0),
        "FLOAT4" => float32(row.try_get::<f32, _>(i)?),
        "FLOAT8" => float(row.try_get::<f64, _>(i)?),
        "NUMERIC" => num(&row.try_get::<NumericText, _>(i)?.0),
        "TEXT" | "VARCHAR" | "BPCHAR" | "CHAR" | "NAME" | "CITEXT" | "UNKNOWN" => {
            Value::String(row.try_get_unchecked::<String, _>(i)?)
        }
        "JSON" | "JSONB" => json_text(row.try_get::<JsonText, _>(i)?)?,
        "UUID" => Value::String(row.try_get::<uuid::Uuid, _>(i)?.to_string()),
        "TIMESTAMPTZ" => Value::String(
            row.try_get::<DateTime<Utc>, _>(i)?
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        "TIMESTAMP" => Value::String(row.try_get::<NaiveDateTime, _>(i)?.to_string()),
        "DATE" => Value::String(row.try_get::<NaiveDate, _>(i)?.to_string()),
        "TIME" => Value::String(row.try_get::<NaiveTime, _>(i)?.to_string()),
        "BYTEA" => Value::String(hex(&row.try_get::<Vec<u8>, _>(i)?)),
        "INTERVAL" => Value::String(interval_text(&row.try_get::<PgInterval, _>(i)?)),
        "FLOAT4[]" => arr(row.try_get::<Vec<Option<f32>>, _>(i)?, float32),
        "DATE[]" => arr(row.try_get::<Vec<Option<NaiveDate>>, _>(i)?, |d| {
            Value::String(d.to_string())
        }),
        "TIMESTAMP[]" => arr(row.try_get::<Vec<Option<NaiveDateTime>>, _>(i)?, |d| {
            Value::String(d.to_string())
        }),
        "TIMESTAMPTZ[]" => arr(row.try_get::<Vec<Option<DateTime<Utc>>>, _>(i)?, |d| {
            Value::String(d.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        }),
        "BOOL[]" => arr(row.try_get::<Vec<Option<bool>>, _>(i)?, Value::Bool),
        "INT2[]" => arr(row.try_get::<Vec<Option<i16>>, _>(i)?, Value::from),
        "INT4[]" => arr(row.try_get::<Vec<Option<i32>>, _>(i)?, Value::from),
        "INT8[]" => arr(row.try_get::<Vec<Option<i64>>, _>(i)?, Value::from),
        "FLOAT8[]" => arr(row.try_get::<Vec<Option<f64>>, _>(i)?, float),
        "NUMERIC[]" => arr(
            row.try_get::<Vec<Option<NumericText>>, _>(i)?,
            |d| num(&d.0),
        ),
        "TEXT[]" | "VARCHAR[]" | "NAME[]" | "BPCHAR[]" => {
            arr(row.try_get::<Vec<Option<String>>, _>(i)?, Value::String)
        }
        "UUID[]" => arr(row.try_get::<Vec<Option<uuid::Uuid>>, _>(i)?, |u| {
            Value::String(u.to_string())
        }),
        "JSONB[]" | "JSON[]" => Value::Array(
            row.try_get::<Vec<Option<JsonText>>, _>(i)?
                .into_iter()
                .map(|x| x.map(json_text).transpose().map(Option::unwrap_or_default))
                .collect::<Result<Vec<Value>>>()?,
        ),
        // an enum's binary form is its label
        _ if matches!(raw.type_info().kind(), PgTypeKind::Enum(_)) => {
            Value::String(row.try_get_unchecked::<String, _>(i)?)
        }
        other => {
            let name = row.columns()[i].name();
            return Err(Error::other(format!(
                "column {name} has type {} that this client does not convert to JSON: cast it in the statement, e.g. {name}::text",
                other.to_lowercase()
            )));
        }
    })
}

/// A numeric as PostgreSQL prints it, with the column's scale (`1.10` stays `1.10`).
struct NumericText(String);

impl sqlx::Type<sqlx::Postgres> for NumericText {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(1700))
    }
    fn compatible(ty: &PgTypeInfo) -> bool {
        ty.name() == "NUMERIC"
    }
}

impl PgHasArrayType for NumericText {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(1231))
    }
    fn array_compatible(ty: &PgTypeInfo) -> bool {
        ty.name() == "NUMERIC[]"
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NumericText {
    fn decode(value: PgValueRef<'r>) -> std::result::Result<Self, sqlx::error::BoxDynError> {
        match value.format() {
            PgValueFormat::Text => Ok(NumericText(value.as_str()?.to_string())),
            PgValueFormat::Binary => Ok(NumericText(numeric_binary_to_text(value.as_bytes()?)?)),
        }
    }
}

/// PostgreSQL's binary numeric (ndigits, weight, sign, dscale, base-10000 digits) as text, the
/// way `numeric_out` prints it.
fn numeric_binary_to_text(b: &[u8]) -> std::result::Result<String, String> {
    let at = |i: usize| -> std::result::Result<i16, String> {
        b.get(i..i + 2)
            .map(|x| i16::from_be_bytes([x[0], x[1]]))
            .ok_or_else(|| "short numeric".to_string())
    };
    let ndigits = at(0)? as i32;
    let weight = at(2)? as i32;
    let sign = at(4)? as u16;
    let dscale = at(6)? as u16 as usize;
    match sign {
        0xC000 => return Ok("NaN".into()),
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        _ => {}
    }
    let digit = |k: i32| -> std::result::Result<i16, String> {
        if k < 0 || k >= ndigits {
            Ok(0)
        } else {
            at(8 + 2 * k as usize)
        }
    };
    let mut s = String::new();
    if sign == 0x4000 {
        s.push('-');
    }
    if weight < 0 {
        s.push('0');
    } else {
        for k in 0..=weight {
            let d = digit(k)?;
            if k == 0 {
                s.push_str(&d.to_string())
            } else {
                s.push_str(&format!("{d:04}"))
            }
        }
    }
    if dscale > 0 {
        let mut frac = String::new();
        let mut k = weight + 1;
        while frac.len() < dscale {
            frac.push_str(&format!("{:04}", digit(k)?));
            k += 1;
        }
        frac.truncate(dscale);
        s.push('.');
        s.push_str(&frac);
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_values_are_cut_not_failed() {
        let deep = |n: usize| format!("{}1{}", "[".repeat(n), "]".repeat(n));
        let v = parse_json_capped(Some(&format!(
            r#"{{"a": "x[{{\"]\\", "b": {}, "c": 1.10}}"#,
            deep(MAX_DEPTH + 50)
        )))
        .unwrap();
        assert_eq!(v["a"], r#"x[{"]\"#);
        assert_eq!(v["c"], "1.10");
        let mut b = &v["b"];
        let mut n = 1;
        while let Some(a) = b.as_array() {
            b = &a[0];
            n += 1;
        }
        assert_eq!(n, MAX_DEPTH, "{b}");
        assert!(b.as_str().unwrap().contains("::text"));
        let two = |s: &str| {
            parse_json_capped(Some(&format!(
                "{}{s}{}",
                "[".repeat(MAX_DEPTH + 5),
                "]".repeat(MAX_DEPTH + 5)
            )))
            .unwrap()
        };
        assert_ne!(two("1"), two("2"), "deep values that differ stay different");
        assert_eq!(two("1"), two("1"));
        assert_eq!(
            parse_json_capped(Some(&deep(MAX_DEPTH))).unwrap(),
            parse_json(Some(&deep(MAX_DEPTH))).unwrap()
        );
    }

    #[test]
    fn lossless() {
        let v = parse_json(Some(
            r#"{"big": 9007199254740993, "d": 1.10, "x": 0.1, "huge": 123456789012345678901234567890, "s": "1.10 \" -5", "a": [-2.5, 1e3]}"#,
        ))
        .unwrap();
        assert_eq!(
            stable_json(&v),
            r#"{"a":[-2.5,"1e3"],"big":9007199254740993,"d":"1.10","huge":"123456789012345678901234567890","s":"1.10 \" -5","x":0.1}"#
        );
        assert_eq!(num("12.50"), Value::String("12.50".into()));
        assert_eq!(num("12.5"), serde_json::json!(12.5));
        assert_eq!(
            parse_json(Some("[-0, 0]")).unwrap().to_string(),
            r#"["-0",0]"#
        );
        for bad in [
            "1.",
            "-",
            "01.5",
            "1e",
            "--1",
            "[1.2.3]",
            "{",
            "{-0:1}",
            "{1.10 : 1}",
        ] {
            assert!(parse_json(Some(bad)).is_err(), "{bad}");
        }
        let deep = |n| format!("{}1.10{}", "[".repeat(n), "]".repeat(n));
        let v = parse_json(Some(&deep(MAX_DEPTH))).unwrap();
        assert!(stable_json(&v).contains("\"1.10\""));
        assert!(v.to_string().contains("\"1.10\""));
        drop(v);
        let e = parse_json(Some(&deep(MAX_DEPTH + 1))).unwrap_err();
        assert!(e.to_string().contains("::text"), "{e}");
        // exact with str::parse must also be exact through serde_json (float_roundtrip)
        for t in ["3106.0529166137962", "-9.313642145546353"] {
            assert_eq!(num(t).to_string(), t);
            assert_eq!(parse_json(Some(t)).unwrap().to_string(), t);
        }
    }

    fn bin(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut v = Vec::new();
        for x in [ndigits, weight, sign as i16, dscale as i16]
            .into_iter()
            .chain(digits.iter().copied())
        {
            v.extend_from_slice(&x.to_be_bytes());
        }
        v
    }

    #[test]
    fn numeric_text() {
        assert_eq!(
            numeric_binary_to_text(&bin(2, 0, 0, 2, &[1, 1000])).unwrap(),
            "1.10"
        );
        assert_eq!(
            numeric_binary_to_text(&bin(2, 1, 0x4000, 0, &[12, 3456])).unwrap(),
            "-123456"
        );
        assert_eq!(
            numeric_binary_to_text(&bin(1, -1, 0, 5, &[5])).unwrap(),
            "0.00050"
        );
        assert_eq!(
            numeric_binary_to_text(&bin(0, 0, 0, 2, &[])).unwrap(),
            "0.00"
        );
        assert_eq!(
            numeric_binary_to_text(&bin(1, 2, 0, 0, &[7])).unwrap(),
            "700000000"
        );
        assert_eq!(
            numeric_binary_to_text(&bin(0, 0, 0xC000, 0, &[])).unwrap(),
            "NaN"
        );
    }

    #[test]
    fn intervals() {
        let iv = |months, days, microseconds| {
            interval_text(&PgInterval {
                months,
                days,
                microseconds,
            })
        };
        assert_eq!(iv(0, 1, 0), "1 day");
        assert_eq!(iv(14, 3, 14_706_500_000), "1 year 2 mons 3 days 04:05:06.5");
        assert_eq!(iv(0, 0, 0), "00:00:00");
        assert_eq!(iv(-1, 0, -3_600_000_000), "-1 mons -01:00:00");
        assert_eq!(iv(0, -1, 3_600_000_000), "-1 days +01:00:00");
        assert_eq!(iv(1, -1, 0), "1 mon -1 days");
        assert_eq!(iv(-13, 2, 0), "-1 years -1 mons +2 days");
        assert_eq!(float32(0.1).to_string(), "0.1");
        assert_eq!(float32(1e20).to_string(), "1e+20");
        assert_eq!(float(f64::NAN), Value::String("NaN".into()));
        assert_eq!(iv(i32::MAX, i32::MAX, i64::MAX), "infinity");
        assert_eq!(iv(i32::MIN, i32::MIN, i64::MIN), "-infinity");
    }

    #[test]
    fn sorted() {
        let v = parse_json(Some(r#"{"b": {"z": 1, "a": 2}, "a": [ {"y":1,"x":2} ]}"#)).unwrap();
        assert_eq!(
            stable_json(&v),
            r#"{"a":[{"x":2,"y":1}],"b":{"a":2,"z":1}}"#
        );
    }
}
