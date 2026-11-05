//! Korean and English. Strings sit next to each other where they are
//! used (`l.t("Nodes", "노드")`), so a page never misses a translation.

use axum::http::HeaderMap;

pub const COOKIE: &str = "pumasi_lang";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lang {
    En,
    Ko,
}

impl Lang {
    pub fn t(self, en: &'static str, ko: &'static str) -> &'static str {
        match self {
            Lang::En => en,
            Lang::Ko => ko,
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Lang::En => "en",
            Lang::Ko => "ko",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "en" => Some(Lang::En),
            "ko" => Some(Lang::Ko),
            _ => None,
        }
    }

    /// The `pumasi_lang` cookie, else the browser's first language.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        if let Some(l) = crate::auth::cookie(headers, COOKIE).and_then(|v| Lang::parse(&v)) {
            return l;
        }
        let accept = headers
            .get(axum::http::header::ACCEPT_LANGUAGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let first = accept.split(',').next().unwrap_or("").trim();
        if first.to_ascii_lowercase().starts_with("ko") {
            Lang::Ko
        } else {
            Lang::En
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn cookie_wins_over_accept_language() {
        let mut h = HeaderMap::new();
        h.insert(
            "accept-language",
            HeaderValue::from_static("ko-KR,ko;q=0.9"),
        );
        assert_eq!(Lang::from_headers(&h), Lang::Ko);
        h.insert("cookie", HeaderValue::from_static("a=b; pumasi_lang=en"));
        assert_eq!(Lang::from_headers(&h), Lang::En);
        let mut h = HeaderMap::new();
        h.insert(
            "accept-language",
            HeaderValue::from_static("en-US,ko;q=0.5"),
        );
        assert_eq!(Lang::from_headers(&h), Lang::En);
        assert_eq!(Lang::from_headers(&HeaderMap::new()), Lang::En);
    }
}
