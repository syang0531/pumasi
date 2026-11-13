//! Cluster events (newest first, live) and alerts: rules, what fires,
//! and the webhooks they go to.

use axum::{
    Form,
    extract::{Path, Query},
    http::HeaderMap,
    response::Response,
};
use maud::{Markup, html};
use pumasi_proto::{
    job::parse_duration_ms,
    manage::Selector,
    monitor::{
        ALERT_RULES, AlertPolicy, AlertTuningView, AlertsView, EventView, FailureRate, SilenceSpec,
        WebhookFormat, WebhookSpec, human_duration, is_off, parse_opt_percent, parse_opt_secs,
    },
    msg::{AdminOp, AdminResponse},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, table};
use crate::fmt;
use crate::i18n::Lang;

#[derive(Deserialize)]
pub struct EventsQuery {
    level: Option<String>,
}

pub async fn page(ctx: Ctx, Query(q): Query<EventsQuery>) -> Response {
    let l = ctx.l;
    let level = q.level.unwrap_or_default();
    let events: Vec<EventView> = match ctx
        .call(AdminOp::Events {
            after: 0,
            since_unix: 0,
            limit: 300,
        })
        .await
    {
        Ok(AdminResponse::Events(e)) => e,
        Ok(_) => Vec::new(),
        Err(e) => return ctx.fail(e, false),
    };
    let alerts = match ctx.call(AdminOp::Alerts).await {
        Ok(AdminResponse::Alerts(a)) => *a,
        _ => AlertsView::default(),
    };
    // Repeat notices, per-node thresholds and silences (1.6).
    let tuning: Option<AlertTuningView> = match ctx.call(AdminOp::AlertTuning).await {
        Ok(AdminResponse::AlertTuning(t)) => Some(*t),
        _ => None,
    };
    let silenced = |rule: &str, subject: &str| {
        tuning
            .as_ref()
            .is_some_and(|t| t.silenced.iter().any(|(r, s)| r == rule && s == subject))
    };
    let admin = ctx.me.is_admin();
    let rows = events
        .iter()
        .rev()
        .filter(|e| level.is_empty() || e.level.to_string() == level)
        .map(|e| {
            html! {
                tr class={"level-" (e.level)} {
                    td { (fmt::time(e.unix)) }
                    td { span class={"badge lvl-" (e.level)} { (e.level.to_string()) } }
                    td { code { (e.kind) } }
                    td { (e.node.clone().unwrap_or_default()) }
                    td { @if let Some(j) = e.job { a href={"/jobs/" (j)} { code { (j) } } } }
                    td { (e.message) }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Events and alerts", "이벤트와 경보")) } }
        nav class="tabs" {
            @for (v, label) in [("", l.t("All", "전체")), ("warning", l.t("Warnings", "경고")), ("critical", l.t("Critical", "심각"))] {
                a href={"/events?level=" (v)} class=[(v == level).then_some("active")] { (label) }
            }
        }
    };
    let live = html! {
        @if !alerts.firing.is_empty() {
            section {
                h2 { (l.t("Firing", "발생 중")) }
                ul class="alerts" {
                    @for a in &alerts.firing {
                        li class={"level-" (a.level)} {
                            strong { (a.rule) } " · " (a.subject) " — " (a.message) " " span class="dim" { (fmt::ago(l, a.since_unix)) }
                            @if silenced(&a.rule, &a.subject) { " " span class="badge" { (l.t("silenced", "묵음")) } }
                        }
                    }
                }
            }
        }
        (table(&[l.t("Time", "시각"), l.t("Level", "수준"), l.t("Kind", "종류"), l.t("Node", "노드"), l.t("Job", "작업"), l.t("Message", "내용")],
            rows, l.t("No events.", "이벤트가 없습니다.")))
    };
    let bottom = html! {
        div class="cols" {
            section {
                h2 { (l.t("Alert rules", "경보 규칙")) }
                dl class="facts" {
                    @for (rule, text) in alerts.policy.describe_in(l == Lang::Ko) { dt { code { (rule) } } dd { (text) } }
                    @if let Some(t) = &tuning {
                        dt { code { "repeat" } }
                        dd { @match t.tuning.repeat_secs {
                            Some(r) => { (l.t("every", "")) " " (human_duration(r)) " " (l.t("while firing", "마다 다시 알림")) }
                            None => { (l.t("off (one notice per alert)", "꺼짐 (경보마다 한 번)")) }
                        } }
                        dt { code { "resolved" } }
                        dd { @if t.tuning.resolved { (l.t("a notice when an alert stops firing", "경보가 그치면 알림")) } @else { (l.t("off", "꺼짐")) } }
                        @for o in &t.tuning.overrides {
                            dt { code { (o.nodes.to_string()) } }
                            dd { (o.describe(l == Lang::Ko)) }
                        }
                    }
                }
                @if admin {
                    (alerts_form(l, &alerts.policy, tuning.as_ref()))
                }
                p class="dim" { (l.t("Also `pumasi alert set` or `[alerts]` in cluster.toml; `pumasi alert set --on NODE` sets one node's thresholds.", "`pumasi alert set`이나 cluster.toml의 `[alerts]`로도 바꿉니다. 노드 하나의 기준은 `pumasi alert set --on 노드`로 바꿉니다.")) }
            }
            @if let Some(t) = &tuning {
                section {
                    h2 { (l.t("Silences", "묵음")) }
                    @if t.silences.is_empty() { p class="empty" { (l.t("None: every alert sends its notices.", "없습니다. 모든 경보가 알림을 보냅니다.")) } }
                    ul class="plain" {
                        @for s in &t.silences {
                            li {
                                strong { (s.describe()) } " · " (l.t("until", "까지:")) " " (fmt::time(s.until_unix)) " · " (s.by)
                                @if !s.comment.is_empty() { br; span class="dim" { (s.comment) } }
                                @if admin { " " button hx-post={"/events/silences/" (s.id) "/end"} hx-target="#flash" { (l.t("End", "끝내기")) } }
                            }
                        }
                    }
                    @if admin {
                        form class="stack" hx-post="/events/silences" hx-target="#flash" {
                            label { (l.t("Rule", "규칙")) " "
                                select name="rule" {
                                    option value="" { (l.t("every rule", "모든 규칙")) }
                                    @for r in ALERT_RULES { option value=(r) { (r) } }
                                }
                            }
                            label { (l.t("Nodes (name or selector, empty: all)", "노드 (이름이나 선택자, 비우면 전체)")) " " input name="on" placeholder="imac, os=windows"; }
                            label { (l.t("For", "기간")) " " input name="for" value="2h" size="6"; }
                            label { (l.t("Comment", "메모")) " " input name="comment"; }
                            button { (l.t("Silence", "묵음 설정")) }
                        }
                    }
                }
            }
            section {
                h2 { (l.t("Webhooks", "웹훅")) }
                @if alerts.webhooks.is_empty() { p class="empty" { (l.t("None yet.", "아직 없습니다.")) } }
                ul class="plain" {
                    @for w in &alerts.webhooks {
                        li {
                            strong { (w.name) } " · " code { (w.target) } " · " (format!("{:?}", w.format).to_lowercase())
                            br; span class="dim" { (w.sent) " " (l.t("sent", "전송")) ", " (w.failed) " " (l.t("failed", "실패"))
                                @if let Some(e) = &w.last_error { " — " (e) } }
                            @if admin {
                                " " button hx-post={"/events/webhooks/" (w.name) "/test"} hx-target="#flash" { (l.t("Send a test", "테스트 보내기")) }
                                " " button class="link danger" hx-post={"/events/webhooks/" (w.name) "/remove"} hx-target="#flash"
                                    hx-confirm=(l.t("Remove this webhook?", "이 웹훅을 지울까요?")) { (l.t("Remove", "지우기")) }
                            }
                        }
                    }
                }
                @if admin {
                    form #webhook-form class="stack" hx-post="/events/webhooks" hx-target="#flash" {
                        label { (l.t("Name", "이름")) " " input name="name" required placeholder="team-chat" pattern="[a-z0-9._-]{1,64}"; }
                        label { "URL " input name="url" type="url" required placeholder="https://hooks.slack.com/services/..."; }
                        label { (l.t("Format", "형식")) " "
                            select name="format" {
                                option value="" { (l.t("guess from the URL", "URL로 추측")) }
                                option value="slack" { "Slack / Mattermost" }
                                option value="discord" { "Discord" }
                                option value="json" { "JSON" }
                            }
                        }
                        button { (l.t("Add webhook", "웹훅 추가")) }
                    }
                }
            }
        }
    };
    ctx.page(
        l.t("Events", "이벤트"),
        Nav::Events,
        top,
        Some(("5s", live)),
        bottom,
    )
}

/// Sends a test alert to a webhook (1.6).
pub async fn test_webhook(ctx: Ctx, headers: HeaderMap, Path(name): Path<String>) -> Response {
    match ctx.call(AdminOp::TestWebhook { name: name.clone() }).await {
        Ok(_) => {
            let msg = format!(
                "{} {name}",
                ctx.l
                    .t("Test alert delivered to", "테스트 경보를 보냈습니다:")
            );
            ctx.done(msg)
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[derive(Deserialize, Default)]
pub struct SilenceForm {
    rule: Option<String>,
    on: Option<String>,
    #[serde(rename = "for")]
    duration: Option<String>,
    comment: Option<String>,
}

/// A node name, or a label selector (`os=windows`).
pub fn node_target(s: &str) -> anyhow::Result<Selector> {
    let s = s.trim();
    let sel: Selector = if s.contains(['=', ',', '!']) {
        s.parse()?
    } else {
        format!("name={s}").parse()?
    };
    sel.validate()?;
    Ok(sel)
}

/// Turns a silence form into a spec.
pub fn silence_spec(
    rule: Option<String>,
    on: Option<String>,
    duration: Option<String>,
    comment: Option<String>,
) -> anyhow::Result<SilenceSpec> {
    let spec = SilenceSpec {
        rule: rule.filter(|r| !r.trim().is_empty()),
        nodes: on
            .as_deref()
            .filter(|o| !o.trim().is_empty())
            .map(node_target)
            .transpose()?,
        secs: parse_duration_ms(duration.as_deref().unwrap_or("2h"))? / 1000,
        comment: comment.unwrap_or_default().trim().to_string(),
    };
    spec.validate()?;
    Ok(spec)
}

pub async fn silence(ctx: Ctx, headers: HeaderMap, Form(f): Form<SilenceForm>) -> Response {
    let spec = match silence_spec(f.rule, f.on, f.duration, f.comment) {
        Ok(s) => s,
        Err(e) => return ctx.fail(e, hx(&headers)),
    };
    let what = format!(
        "{} ({})",
        spec.rule.as_deref().unwrap_or("*"),
        human_duration(spec.secs)
    );
    match ctx.call(AdminOp::Silence(Box::new(spec))).await {
        Ok(_) => {
            let msg = format!("{} {what}", ctx.l.t("Silenced:", "묵음을 설정했습니다:"));
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn unsilence(ctx: Ctx, headers: HeaderMap, Path(id): Path<String>) -> Response {
    match ctx.call(AdminOp::Unsilence { id }).await {
        Ok(_) => {
            let msg = ctx.l.t("Silence ended.", "묵음을 끝냈습니다.").to_string();
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

/// The silences are outside the live region: show the page again.
fn reload(mut r: Response) -> Response {
    r.headers_mut()
        .insert("hx-refresh", axum::http::HeaderValue::from_static("true"));
    r
}

/// The alert rules and notice settings, as a form (1.7). Each threshold
/// is text: empty or `off` turns its rule off.
fn alerts_form(l: Lang, p: &AlertPolicy, t: Option<&AlertTuningView>) -> Markup {
    let secs = |s: Option<u64>| s.map(human_duration).unwrap_or_else(|| "off".into());
    let pct = |v: Option<u8>| v.map(|v| format!("{v}%")).unwrap_or_else(|| "off".into());
    let f = p.failure_rate;
    html! {
        form #alerts-form class="stack settings" hx-post="/events/alerts" hx-target="#flash"
            hx-confirm=(l.t("Save the alert rules?", "경보 규칙을 저장할까요?")) {
            p class="dim" { (l.t("Empty or `off` turns a rule off.", "비우거나 `off`로 두면 규칙이 꺼집니다.")) }
            div class="grid2" {
                label { (l.t("Node offline for", "노드 오프라인 시간")) input name="node_offline" value=(secs(p.node_offline_secs)); }
                label { (l.t("Disk fuller than", "디스크 사용률")) input name="disk" value=(pct(p.disk_percent)); }
                label { (l.t("Failed runs above", "실패율")) input name="failure_rate" value=(f.map(|f| format!("{}%", f.percent)).unwrap_or_else(|| "off".into())); }
                label { (l.t("… over", "… 기간")) input name="failure_window" value=(human_duration(f.unwrap_or_default().window_secs)); }
                label { (l.t("… of at least (runs)", "… 최소 실행 수")) input name="failure_min_runs" type="number" min="1" value=(f.unwrap_or_default().min_runs); }
                label { (l.t("Hotter than (°C)", "온도 (°C)")) input name="temperature" value=(p.temp_c.map(|c| c.to_string()).unwrap_or_else(|| "off".into())); }
                label { (l.t("Benchmark slower by", "벤치마크 하락")) input name="perf_drop" value=(pct(p.perf_drop_percent)); }
                @if let Some(t) = t {
                    label { (l.t("Repeat a notice every", "알림 반복 간격")) input name="repeat" value=(t.tuning.repeat_secs.map(human_duration).unwrap_or_else(|| "off".into())); }
                }
            }
            label class="check" { input type="checkbox" name="smart" value="on" checked[p.smart]; " " (l.t("A drive's SMART health failing", "드라이브 SMART 이상")) }
            label class="check" { input type="checkbox" name="check_mismatch" value="on" checked[p.check_mismatch]; " " (l.t("A check run disagreeing with a result", "검증 실행이 결과와 다름")) }
            @if let Some(t) = t {
                label class="check" { input type="checkbox" name="resolved" value="on" checked[t.tuning.resolved]; " " (l.t("A notice when an alert stops firing", "경보가 그치면 알림")) }
            }
            input type="hidden" name="tuning" value=(if t.is_some() { "1" } else { "" });
            div class="row" { button type="submit" { (l.t("Save rules", "규칙 저장")) } }
        }
    }
}

#[derive(Deserialize, Default)]
pub struct AlertsForm {
    node_offline: Option<String>,
    failure_rate: Option<String>,
    failure_window: Option<String>,
    failure_min_runs: Option<String>,
    disk: Option<String>,
    temperature: Option<String>,
    perf_drop: Option<String>,
    smart: Option<String>,
    check_mismatch: Option<String>,
    repeat: Option<String>,
    resolved: Option<String>,
    tuning: Option<String>,
}

fn blank_off(v: &Option<String>) -> String {
    match v.as_deref().map(str::trim) {
        None | Some("") => "off".into(),
        Some(s) => s.to_string(),
    }
}

/// The repeat interval and the resolved switch of the alerts form.
pub type Notices = (Option<u64>, bool);

/// Turns the form into a policy (and, when the form had them, the repeat
/// and resolved settings).
pub fn alerts_from(f: &AlertsForm) -> anyhow::Result<(AlertPolicy, Option<Notices>)> {
    use anyhow::Context as _;
    let base = FailureRate::default();
    let failure_rate = parse_opt_percent(&blank_off(&f.failure_rate))
        .context("failed runs")?
        .map(|percent| -> anyhow::Result<FailureRate> {
            Ok(FailureRate {
                percent,
                window_secs: parse_opt_secs(&blank_off(&f.failure_window))
                    .context("failure window")?
                    .unwrap_or(base.window_secs),
                min_runs: match f.failure_min_runs.as_deref().map(str::trim) {
                    None | Some("") => base.min_runs,
                    Some(n) => n.parse().context("minimum runs: a whole number")?,
                },
            })
        })
        .transpose()?;
    let temp = blank_off(&f.temperature);
    let p = AlertPolicy {
        node_offline_secs: parse_opt_secs(&blank_off(&f.node_offline)).context("node offline")?,
        failure_rate,
        disk_percent: parse_opt_percent(&blank_off(&f.disk)).context("disk")?,
        temp_c: if is_off(&temp) {
            None
        } else {
            Some(
                temp.trim_end_matches('C')
                    .trim_end_matches('°')
                    .trim()
                    .parse()
                    .context("temperature: degrees Celsius")?,
            )
        },
        perf_drop_percent: parse_opt_percent(&blank_off(&f.perf_drop)).context("benchmark drop")?,
        smart: f.smart.is_some(),
        check_mismatch: f.check_mismatch.is_some(),
    };
    let tuning = if f.tuning.as_deref().is_some_and(|t| !t.is_empty()) {
        Some((
            parse_opt_secs(&blank_off(&f.repeat)).context("repeat")?,
            f.resolved.is_some(),
        ))
    } else {
        None
    };
    Ok((p, tuning))
}

pub async fn save_alerts(ctx: Ctx, headers: HeaderMap, Form(f): Form<AlertsForm>) -> Response {
    let res = async {
        let (policy, tuning) = alerts_from(&f)?;
        ctx.call(AdminOp::SetAlertPolicy(Box::new(policy))).await?;
        if let Some((repeat_secs, resolved)) = tuning {
            // Keep the per-node overrides as they are.
            let mut t = match ctx.call(AdminOp::AlertTuning).await? {
                AdminResponse::AlertTuning(v) => v.tuning,
                _ => anyhow::bail!("unexpected response"),
            };
            t.repeat_secs = repeat_secs;
            t.resolved = resolved;
            ctx.call(AdminOp::SetAlertTuning(Box::new(t))).await?;
        }
        anyhow::Ok(())
    }
    .await;
    match res {
        Ok(()) => {
            let msg = ctx
                .t("Alert rules saved.", "경보 규칙을 저장했습니다.")
                .to_string();
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

#[derive(Deserialize)]
pub struct WebhookForm {
    name: String,
    url: String,
    format: Option<String>,
}

pub async fn add_webhook(ctx: Ctx, headers: HeaderMap, Form(f): Form<WebhookForm>) -> Response {
    let res = async {
        let url = f.url.trim().to_string();
        let format = match f.format.as_deref().map(str::trim) {
            None | Some("") => WebhookFormat::detect(&url),
            Some(s) => s.parse()?,
        };
        let spec = WebhookSpec {
            name: f.name.trim().to_string(),
            url,
            format,
        };
        spec.validate()?;
        ctx.call(AdminOp::SetWebhook(Box::new(spec))).await?;
        anyhow::Ok(())
    }
    .await;
    match res {
        Ok(()) => {
            let msg = format!(
                "{} {}",
                ctx.t("Webhook saved:", "웹훅을 저장했습니다:"),
                f.name.trim()
            );
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

pub async fn remove_webhook(ctx: Ctx, headers: HeaderMap, Path(name): Path<String>) -> Response {
    match ctx
        .call(AdminOp::RemoveWebhook { name: name.clone() })
        .await
    {
        Ok(_) => {
            let msg = format!("{} {name}", ctx.t("Webhook removed:", "웹훅을 지웠습니다:"));
            reload(ctx.done(msg))
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
