//! Top-level statements of a script, aware of quotes, dollar quotes and comments (a port of the
//! splitter every gnomon SDK uses). A run refuses a second statement in one call whatever the
//! driver does.

/// Split a script into its top-level statements. Empty statements (only comments or `;`) are dropped.
pub fn split_sql(sql: &str) -> Vec<String> {
    let s = sql.as_bytes();
    let n = s.len();
    let mut out: Vec<String> = Vec::new();
    let mut start = 0usize; // start of the current statement in `sql`
    let mut i = 0usize;
    let find = |from: usize, pat: &[u8]| -> Option<usize> {
        if from > n {
            return None;
        }
        s[from..]
            .windows(pat.len())
            .position(|w| w == pat)
            .map(|p| p + from)
    };
    while i < n {
        let c = s[i];
        if c == b'-' && s.get(i + 1) == Some(&b'-') {
            i = find(i, b"\n").unwrap_or(n);
            continue;
        }
        if c == b'/' && s.get(i + 1) == Some(&b'*') {
            i = find(i + 2, b"*/").map(|j| j + 2).unwrap_or(n);
            continue;
        }
        if c == b'\'' {
            let mut j = i + 1;
            while j < n {
                if s[j] == b'\'' && s.get(j + 1) == Some(&b'\'') {
                    j += 2;
                    continue;
                }
                if s[j] == b'\'' {
                    break;
                }
                j += 1;
            }
            i = (j + 1).min(n);
            continue;
        }
        if c == b'"' {
            i = find(i + 1, b"\"").map(|j| j + 1).unwrap_or(n);
            continue;
        }
        if c == b'$'
            && let Some(tag_len) = dollar_tag(&s[i..])
        {
            let tag = &s[i..i + tag_len];
            i = find(i + tag_len, tag).map(|j| j + tag_len).unwrap_or(n);
            continue;
        }
        if c == b';' {
            out.push(sql[start..=i].to_string());
            i += 1;
            start = i;
            continue;
        }
        i += 1;
    }
    if !sql[start..].trim().is_empty() {
        out.push(sql[start..].to_string());
    }
    out.into_iter()
        .filter(|st| !is_empty_statement(st))
        .collect()
}

/// The length of a dollar-quote tag (`$$` or `$name$`) at the start of `s`.
fn dollar_tag(s: &[u8]) -> Option<usize> {
    if s.first() != Some(&b'$') {
        return None;
    }
    let mut j = 1;
    if let Some(&c) = s.get(1)
        && (c.is_ascii_alphabetic() || c == b'_')
    {
        j = 2;
        while let Some(&c) = s.get(j) {
            if c.is_ascii_alphanumeric() || c == b'_' {
                j += 1;
            } else {
                break;
            }
        }
    }
    (s.get(j) == Some(&b'$')).then_some(j + 1)
}

/// Only comments, whitespace and semicolons.
fn is_empty_statement(st: &str) -> bool {
    let mut rest = String::new();
    let b = st.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
            match st[i + 2..].find("*/") {
                Some(p) => i = i + 2 + p + 2,
                None => {
                    // an unterminated comment is kept as text, as the reference splitter does
                    rest.push_str(&st[i..]);
                    break;
                }
            }
            continue;
        }
        let ch = st[i..].chars().next().unwrap();
        if ch != ';' {
            rest.push(ch);
        }
        i += ch.len_utf8();
    }
    rest.trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::split_sql;

    #[test]
    fn splits() {
        assert_eq!(split_sql("select 1; select 2").len(), 2);
        assert_eq!(split_sql("select 1;").len(), 1);
        assert_eq!(split_sql("select 1; -- trailing\n").len(), 1);
        assert_eq!(split_sql("select ';'; select 2").len(), 2);
        assert_eq!(split_sql("select 'it''s; fine'").len(), 1);
        assert_eq!(split_sql("select \"a;b\" from t").len(), 1);
        assert_eq!(split_sql("select $$ a; b $$").len(), 1);
        assert_eq!(split_sql("select $tag$ a; $$ b; $tag$").len(), 1);
        assert_eq!(split_sql("select 1 /* ; */").len(), 1);
        assert_eq!(split_sql("select $1, $2").len(), 1);
        assert_eq!(split_sql("; ;  -- x\n").len(), 0);
        assert_eq!(
            split_sql("select 'é'; select 'ü'"),
            vec!["select 'é';", " select 'ü'"]
        );
    }
}
