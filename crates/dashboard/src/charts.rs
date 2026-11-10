//! Line charts drawn on the server as SVG: no chart library, no script,
//! and they follow the page's colours (CSS classes, light and dark).

use maud::{Markup, PreEscaped, html};

pub struct Series {
    pub label: String,
    pub points: Vec<(u64, f64)>,
}

const W: f64 = 640.0;
const H: f64 = 170.0;
const LEFT: f64 = 72.0;
const RIGHT: f64 = 8.0;
const TOP: f64 = 10.0;
const BOTTOM: f64 = 22.0;

/// A round upper bound for the axis: 1, 2 or 5 times a power of ten.
pub fn nice_max(v: f64) -> f64 {
    if v <= 0.0 || !v.is_finite() {
        return 1.0;
    }
    let p = 10f64.powf(v.log10().floor());
    for m in [1.0, 2.0, 5.0, 10.0] {
        if v <= m * p {
            return m * p;
        }
    }
    10.0 * p
}

/// `fixed_max`: the top of the axis (100 for percentages), else from the
/// data. `step`: seconds between points; longer gaps break the line.
pub fn line_chart(
    title: &str,
    series: &[Series],
    fixed_max: Option<f64>,
    step: u64,
    (from, to): (u64, u64),
    fmt: &dyn Fn(f64) -> String,
) -> Markup {
    let data_max = series
        .iter()
        .flat_map(|s| s.points.iter().map(|p| p.1))
        .fold(0.0f64, f64::max);
    let max = fixed_max.unwrap_or_else(|| nice_max(data_max));
    let span = to.saturating_sub(from).max(1) as f64;
    let x = |t: u64| LEFT + (t.saturating_sub(from) as f64 / span).min(1.0) * (W - LEFT - RIGHT);
    let y = |v: f64| TOP + (1.0 - (v / max).clamp(0.0, 1.0)) * (H - TOP - BOTTOM);
    let empty = series.iter().all(|s| s.points.is_empty());
    let mut paths = String::new();
    for (i, s) in series.iter().enumerate() {
        let mut d = String::new();
        let mut last: Option<u64> = None;
        for &(t, v) in &s.points {
            let gap = last.is_none_or(|l| t.saturating_sub(l) > step.max(1) * 3);
            d.push_str(&format!(
                "{}{:.1},{:.1} ",
                if gap { "M" } else { "L" },
                x(t),
                y(v)
            ));
            last = Some(t);
        }
        if !d.is_empty() {
            paths.push_str(&format!(
                r#"<path class="line s{i}" d="{}"><title>{}</title></path>"#,
                d.trim_end(),
                maud::html! { (s.label) }.into_string()
            ));
            // A lone point has no line: mark it.
            if s.points.len() == 1 {
                let (t, v) = s.points[0];
                paths.push_str(&format!(
                    r#"<circle class="dot s{i}" cx="{:.1}" cy="{:.1}" r="2.5"/>"#,
                    x(t),
                    y(v)
                ));
            }
        }
    }
    html! {
        figure class="chart" {
            figcaption {
                strong { (title) }
                @for (i, s) in series.iter().enumerate() {
                    span class={"key s" (i)} {
                        (s.label)
                        @if let Some(&(_, v)) = s.points.last() { " " b { (fmt(v)) } }
                    }
                }
            }
            svg viewBox=(format!("0 0 {W} {H}")) role="img" aria-label=(title) {
                @for frac in [0.0, 0.5, 1.0] {
                    @let yy = y(max * frac);
                    line class="grid" x1=(LEFT) x2=(W - RIGHT) y1=(format!("{yy:.1}")) y2=(format!("{yy:.1}")) {}
                    text class="axis" x=(LEFT - 6.0) y=(format!("{:.1}", yy + 4.0)) text-anchor="end" { (fmt(max * frac)) }
                }
                // UTC here; the browser turns them into local time (app.js).
                text class="axis" x=(LEFT) y=(H - 6.0) data-hm=(crate::fmt::iso_utc(from)) { (crate::fmt::iso_utc(from)[11..16]) " UTC" }
                text class="axis" x=(W - RIGHT) y=(H - 6.0) text-anchor="end" data-hm=(crate::fmt::iso_utc(to)) { (crate::fmt::iso_utc(to)[11..16]) " UTC" }
                (PreEscaped(paths))
                @if empty {
                    text class="axis" x=(W / 2.0) y=(H / 2.0) text-anchor="middle" { "—" }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axis_tops_are_round() {
        assert_eq!(nice_max(0.0), 1.0);
        assert_eq!(nice_max(0.7), 1.0);
        assert_eq!(nice_max(1.5), 2.0);
        assert_eq!(nice_max(37.0), 50.0);
        assert_eq!(nice_max(51.0), 100.0);
        assert_eq!(nice_max(8_000_000.0), 10_000_000.0);
    }

    #[test]
    fn gaps_break_the_line() {
        let s = Series {
            label: "cpu <&>".into(),
            points: vec![(0, 10.0), (60, 20.0), (600, 30.0)],
        };
        let svg =
            line_chart("t", &[s], Some(100.0), 60, (0, 600), &|v| format!("{v}")).into_string();
        assert_eq!(svg.matches('M').count(), 2, "{svg}");
        assert!(svg.contains("cpu &lt;&amp;&gt;"));
    }
}
