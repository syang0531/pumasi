//! Small pieces of HTML shared by the pages.

use maud::{Markup, html};
use pumasi_proto::msg::{JobState, NodeView, TaskState};

use crate::i18n::Lang;

pub use pumasi_proto::{economy::format_cu, monitor::human_duration, store::format_size};

/// A UTC time; `app.js` rewrites it into the browser's time zone.
pub fn time(unix: u64) -> Markup {
    if unix == 0 {
        return html! { "—" };
    }
    let iso = iso_utc(unix);
    html! { time datetime=(iso) { (iso.replace('T', " ").trim_end_matches('Z')) " UTC" } }
}

/// `2026-11-05T11:20:00Z`.
pub fn iso_utc(unix: u64) -> String {
    let days = (unix / 86400) as i64;
    let secs = unix % 86400;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// "5m ago".
pub fn ago(l: Lang, unix: u64) -> String {
    if unix == 0 {
        return "—".into();
    }
    let secs = now_unix().saturating_sub(unix);
    match l {
        Lang::En => format!("{} ago", human_duration(secs)),
        Lang::Ko => format!("{} 전", human_duration(secs)),
    }
}

pub fn ms(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.1} s", ms as f64 / 1000.0)
    } else {
        human_duration(ms / 1000)
    }
}

pub fn cu(m: i64) -> String {
    format!("{} CU", format_cu(m))
}

pub fn job_state(l: Lang, s: JobState) -> Markup {
    let (class, text) = match s {
        JobState::Queued => ("muted", l.t("queued", "대기")),
        JobState::Running => ("info", l.t("running", "실행 중")),
        JobState::Succeeded => ("ok", l.t("succeeded", "성공")),
        JobState::Failed => ("bad", l.t("failed", "실패")),
        JobState::Cancelled => ("warn", l.t("cancelled", "취소됨")),
    };
    html! { span class={"badge " (class)} data-state=(s.to_string()) { (text) } }
}

pub fn task_state(l: Lang, s: &TaskState) -> Markup {
    let (class, text, extra) = match s {
        TaskState::Pending => ("muted", l.t("pending", "대기"), None),
        TaskState::Running { node } => ("info", l.t("running", "실행 중"), Some(node.clone())),
        TaskState::Succeeded => ("ok", l.t("succeeded", "성공"), None),
        TaskState::Failed(o) => ("bad", l.t("failed", "실패"), Some(o.to_string())),
        TaskState::Cancelled => ("warn", l.t("cancelled", "취소됨"), None),
        TaskState::Verifying => ("info", l.t("verifying", "검증 중"), None),
    };
    html! {
        span class={"badge " (class)} { (text) }
        @if let Some(e) = extra { " " span class="dim" { (e) } }
    }
}

/// Online, offline, paused, cordoned, suspended.
pub fn node_state(l: Lang, n: &NodeView) -> Markup {
    html! {
        @if n.online {
            span class="badge ok" data-state="online" { (l.t("online", "온라인")) }
        } @else {
            span class="badge bad" data-state="offline" { (l.t("offline", "오프라인")) }
        }
        @if n.config.cordoned {
            " " span class="badge warn" data-state="cordoned" { (l.t("cordoned", "차단됨")) }
        }
        @if n.suspended {
            " " span class="badge bad" { (l.t("suspended", "정지됨")) }
        }
        @if let Some(p) = &n.paused {
            " " span class="badge muted" title=(p) { (l.t("paused", "일시 중지")) }
        }
    }
}

/// A bar for a share, 0 to 100.
pub fn meter(percent: f32, label: String) -> Markup {
    let p = percent.clamp(0.0, 100.0);
    let class = if p >= 90.0 {
        "hi"
    } else if p >= 70.0 {
        "mid"
    } else {
        "lo"
    };
    html! {
        span class="meter-wrap" {
            meter class=(class) min="0" max="100" low="70" high="90" optimum="0" value=(format!("{p:.0}")) {}
            span class="meter-label" { (label) }
        }
    }
}

pub fn percent(used: u64, total: u64) -> f32 {
    if total == 0 {
        0.0
    } else {
        used as f32 * 100.0 / total as f32
    }
}

pub fn short(id: &str) -> &str {
    &id[..id.len().min(8)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_dates() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(iso_utc(1_793_877_679), "2026-11-05T11:21:19Z");
    }
}
