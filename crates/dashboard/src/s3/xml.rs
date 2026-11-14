//! The little XML the S3 API needs: escaping what the gateway writes, and
//! picking elements out of the few bodies clients send (completing a
//! multipart upload, deleting several objects).

/// The namespace of S3 responses.
pub const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // Characters XML 1.0 cannot carry at all.
            c if (c as u32) < 0x20 && !matches!(c, '\t' | '\n' | '\r') => {
                out.push_str(&format!("&#x{:X};", c as u32))
            }
            c => out.push(c),
        }
    }
    out
}

pub fn unesc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(end) = rest.find(';') else {
            break;
        };
        let ent = &rest[1..end];
        let c = match ent {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => ent
                .strip_prefix("#x")
                .or_else(|| ent.strip_prefix("#X"))
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| ent.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match c {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// The contents of every `<tag>` element in `xml`, in order (elements of
/// the same name must not nest).
pub fn all<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut rest = xml;
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        // `<Key>` but not `<KeyMarker>`.
        let Some(c) = after.chars().next() else {
            break;
        };
        if c != '>' && c != ' ' && c != '/' && c != '\t' && c != '\n' && c != '\r' {
            rest = after;
            continue;
        }
        let Some(gt) = after.find('>') else {
            break;
        };
        if after[..gt].ends_with('/') {
            out.push("");
            rest = &after[gt + 1..];
            continue;
        }
        let body = &after[gt + 1..];
        let Some(end) = body.find(&close) else {
            break;
        };
        out.push(&body[..end]);
        rest = &body[end + close.len()..];
    }
    out
}

/// The text of the first `<tag>` element, unescaped.
pub fn text(xml: &str, tag: &str) -> Option<String> {
    all(xml, tag).first().map(|s| unesc(s.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_elements() {
        let x = r#"<?xml version="1.0"?><CompleteMultipartUpload xmlns="x">
            <Part><PartNumber>1</PartNumber><ETag>"a&quot;b"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>&quot;c&quot;</ETag></Part>
            </CompleteMultipartUpload>"#;
        let parts = all(x, "Part");
        assert_eq!(parts.len(), 2);
        assert_eq!(text(parts[1], "PartNumber").as_deref(), Some("2"));
        assert_eq!(text(parts[1], "ETag").as_deref(), Some("\"c\""));
        assert_eq!(all("<KeyMarker>x</KeyMarker><Key>k</Key>", "Key"), ["k"]);
        assert_eq!(esc("a<&>\"'\u{1}"), "a&lt;&amp;&gt;&quot;&apos;&#x1;");
        assert_eq!(unesc("&lt;&#65;&#x42;&bogus;&"), "<AB&bogus;&");
    }
}
