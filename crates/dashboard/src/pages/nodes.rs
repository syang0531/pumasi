//! Nodes: the list with live metrics, and one node's charts, benchmark,
//! GPUs, labels and the admin's buttons (cordon, drain, benchmark).

use axum::{
    Form,
    extract::{Path, Query},
    http::HeaderMap,
    response::Response,
};
use maud::{Markup, html};
use pumasi_proto::{
    manage::NodeEdit,
    monitor::{MetricsView, NetView, NodeMetricsView, human_bps, human_rtt},
    msg::{AdminOp, AdminResponse, NodeView},
    power::{NodePowerView, format_mac},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, status, table};
use crate::{
    charts::{Series, line_chart},
    fmt::{self, format_size, meter, percent},
    i18n::Lang,
};

async fn metrics(ctx: &Ctx, node: Option<String>, since_secs: u64) -> MetricsView {
    match ctx.call(AdminOp::Metrics { node, since_secs }).await {
        Ok(AdminResponse::Metrics(m)) => *m,
        _ => MetricsView::default(),
    }
}

/// What each node does about sleep and whether it is asleep (1.5). Empty
/// for a control plane older than 1.5.
async fn power(ctx: &Ctx) -> Vec<NodePowerView> {
    match ctx.call(AdminOp::NodePower).await {
        Ok(AdminResponse::NodePower(p)) => p,
        _ => Vec::new(),
    }
}

pub async fn list(ctx: Ctx) -> Response {
    match render_list(&ctx).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render_list(ctx: &Ctx) -> anyhow::Result<Response> {
    let l = ctx.l;
    let s = status(ctx).await?;
    let m = metrics(ctx, None, 0).await;
    let power = power(ctx).await;
    let mut nodes = s.nodes.clone();
    nodes.sort_by(|a, b| b.online.cmp(&a.online).then(a.info.name.cmp(&b.info.name)));
    let rows = nodes
        .iter()
        .map(|n| {
            let mv = m.nodes.iter().find(|x| x.id == n.id);
            let asleep = power
                .iter()
                .any(|p| p.id == n.id && p.asleep_since_unix.is_some());
            row(l, n, mv, asleep)
        })
        .collect();
    let live = table(
        &[
            l.t("Name", "이름"),
            l.t("State", "상태"),
            l.t("System", "시스템"),
            l.t("Role", "역할"),
            l.t("Tasks", "태스크"),
            "CPU",
            l.t("Memory", "메모리"),
            l.t("Disk", "디스크"),
            l.t("Temp", "온도"),
            "GPU",
            l.t("CPU score", "CPU 점수"),
            l.t("Labels", "라벨"),
        ],
        rows,
        l.t(
            "No nodes yet. Invite one from the Invites page or run `pumasi token` and `pumasi agent join`.",
            "아직 노드가 없습니다. 초대 화면에서 티켓을 만들거나 `pumasi token`과 `pumasi agent join`을 쓰세요.",
        ),
    );
    let conf = match ctx.call(AdminOp::Confidential).await {
        Ok(AdminResponse::Confidential(v)) => confidential(ctx, &v),
        _ => html! {},
    };
    let net = match ctx.call(AdminOp::Net).await {
        Ok(AdminResponse::Net(v)) => net_matrix(l, &v),
        _ => html! {},
    };
    Ok(ctx.page(
        l.t("Nodes", "노드"),
        Nav::Nodes,
        html! { div class="page-head" { h1 { (l.t("Nodes", "노드")) } } },
        Some(("5s", live)),
        html! { (super::upgrade::placeholder(ctx)) (net) (conf) },
    ))
}

/// Round trips between nodes as a matrix (row: from, column: to), with
/// the bandwidth under each (1.6).
fn net_matrix(l: Lang, v: &NetView) -> Markup {
    let nodes: Vec<&String> = v
        .nodes
        .iter()
        .filter(|n| v.links.iter().any(|x| x.from == **n || x.to == **n))
        .collect();
    html! {
        section class="card" id="net" {
            h2 { (l.t("Network between nodes", "노드 사이 네트워크")) }
            @if nodes.is_empty() {
                p class="empty" { (l.t(
                    "Nothing measured yet. Agents of 1.6 or later measure each other now and then.",
                    "아직 잰 것이 없습니다. 1.6 이상의 에이전트가 가끔 서로를 잽니다.")) }
            } @else {
                p class="dim" { (l.t("Round trip and bandwidth, row to column.", "왕복 시간과 대역폭 (행에서 열로).")) }
                div class="table-wrap" {
                    table class="matrix" {
                        thead { tr { th {} @for n in &nodes { th { (n) } } } }
                        tbody {
                            @for a in &nodes {
                                tr {
                                    th { (a) }
                                    @for b in &nodes {
                                        @if a == b { td class="dim" { "-" } }
                                        @else {
                                            @match v.link(a, b) {
                                                Some(x) if x.error.is_some() => {
                                                    td class="bad" title=(x.error.clone().unwrap_or_default()) { (l.t("failed", "실패")) }
                                                }
                                                Some(x) => {
                                                    td title=(format!("{} · {}", fmt::ago(l, x.unix), match x.direct { Some(false) => "relay", _ => "direct" })) {
                                                        (x.rtt_us.map(human_rtt).unwrap_or_default())
                                                        @if let Some(b) = x.bps { br; span class="dim" { (human_bps(b)) } }
                                                    }
                                                }
                                                None => { td {} }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The confidential tier: which nodes start TEE VMs, and the launch
/// measurements their guests attested with (an admin trusts them here).
fn confidential(ctx: &Ctx, v: &pumasi_proto::confidential::ConfidentialView) -> Markup {
    let l = ctx.l;
    let tee_nodes: Vec<_> = v
        .nodes
        .iter()
        .filter(|n| !n.tees.is_empty() || n.unavailable.is_some() || n.trusted_by_admin)
        .collect();
    if tee_nodes.is_empty() && v.measurements.is_empty() && !v.test_tee {
        return html! {};
    }
    let admin = ctx.me.is_admin();
    html! {
        section class="card" {
            h2 { (l.t("Confidential tier", "기밀 등급")) }
            @if v.test_tee {
                p class="warn" { (l.t(
                    "The control plane accepts the test TEE (--insecure-test-tee): nothing confidential is protected.",
                    "컨트롤 플레인이 테스트용 TEE(--insecure-test-tee)를 받습니다. 기밀 작업이 보호되지 않습니다.")) }
            }
            @if !tee_nodes.is_empty() {
                (table(&[l.t("Node", "노드"), "TEE", l.t("Trusted host", "신뢰 호스트"), ""],
                    tee_nodes.iter().map(|n| html! {
                        tr {
                            td { a href={"/nodes/" (n.id)} { (n.name) } }
                            td {
                                @if n.tees.is_empty() { span class="dim" { (n.unavailable.as_deref().unwrap_or("—")) } }
                                @else { (n.tees.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ")) }
                            }
                            td { @if n.trusted_host { (l.t("yes", "예")) } @else { (l.t("no", "아니오")) } }
                            td {
                                @if admin && n.trusted_by_admin {
                                    button hx-post={"/nodes/" (n.id) "/untrust"} hx-target="#flash" { (l.t("Untrust", "신뢰 해제")) }
                                }
                            }
                        }
                    }).collect(), ""))
            }
            h3 { (l.t("Launch measurements", "부팅 측정값")) }
            p class="dim" { (l.t(
                "Keys go only to guests whose measurement is trusted. Trust a measurement only for a guest you expect: this agent version's firmware, kernel and guest init.",
                "측정값을 신뢰한 게스트에만 키를 줍니다. 예상한 게스트(이 에이전트 버전의 펌웨어·커널·게스트 init)의 측정값만 신뢰하세요.")) }
            (table(&[l.t("Measurement", "측정값"), "TEE", l.t("Trusted", "신뢰"), l.t("Seen", "횟수"), l.t("Node", "노드"), l.t("Details", "내용"), ""],
                v.measurements.iter().map(|m| html! {
                    tr {
                        td { code title=(m.id) { (&m.id[..m.id.len().min(16)]) } }
                        td { (m.tee.to_string()) }
                        td { @if m.trusted { (l.t("yes", "예")) } @else { (l.t("no", "아니오")) } }
                        td { (m.seen) }
                        td { (m.node) }
                        td { span class="dim" { @if m.note.is_empty() { "agent " (m.agent_version) ", " (m.vcpus) " vCPU; " (m.detail) } @else { (m.note) } } }
                        td {
                            @if admin {
                                @if m.trusted {
                                    button hx-post={"/nodes/" (m.id) "/untrust-measurement"} hx-target="#flash" { (l.t("Untrust", "신뢰 해제")) }
                                } @else {
                                    button class="warn" hx-post={"/nodes/" (m.id) "/trust-measurement"} hx-target="#flash"
                                        hx-confirm=(l.t("Trust this launch measurement? Guests that attest with it get the keys of confidential jobs.",
                                                        "이 부팅 측정값을 신뢰할까요? 이 값으로 증명한 게스트가 기밀 작업의 키를 받습니다."))
                                        { (l.t("Trust", "신뢰")) }
                                }
                            }
                        }
                    }
                }).collect(),
                l.t("None seen yet: they appear once a confidential task attests.", "아직 없습니다. 기밀 태스크가 증명하면 나타납니다.")))
        }
    }
}

fn row(l: Lang, n: &NodeView, mv: Option<&NodeMetricsView>, asleep: bool) -> Markup {
    let latest = mv.and_then(|m| m.latest.as_ref());
    html! {
        tr data-node=(n.info.name) {
            td { a href={"/nodes/" (n.id)} { strong { (n.info.name) } } br; code class="dim" { (fmt::short(&n.id)) } }
            td { (fmt::node_state(l, n, asleep)) }
            td { (n.info.os) "/" (n.info.arch) br; span class="dim" { (n.info.cpus) " CPU · " (format_size(n.info.memory_bytes)) } }
            td { (n.role.to_string()) br; span class="dim" { (n.account) } }
            td { (n.running) " / " (n.info.slots) }
            td { @if let Some(x) = latest { (meter(x.cpu_percent, format!("{:.0}%", x.cpu_percent))) } @else { "—" } }
            td { @if let Some(x) = latest {
                (meter(percent(x.memory_used, x.memory_total), format!("{} / {}", format_size(x.memory_used), format_size(x.memory_total))))
            } @else { "—" } }
            td { @if let Some(x) = latest {
                (meter(x.disk_percent().unwrap_or(0.0), format!("{:.0}%", x.disk_percent().unwrap_or(0.0))))
            } @else { "—" } }
            td { @if let Some(t) = latest.and_then(|x| x.hottest()) { (format!("{t:.0} °C")) } @else { "—" } }
            td {
                @if n.gpus.is_empty() { "—" } @else {
                    (n.gpus.iter().filter(|g| g.busy.is_some()).count()) " / " (n.gpus.len())
                    @if let Some(x) = latest { @if !x.gpus.is_empty() {
                        br; span class="dim" { (format!("{:.0}%", x.gpus.iter().map(|g| g.util_percent).sum::<f32>() / x.gpus.len() as f32)) }
                    } }
                }
            }
            td { (n.cpu_score) }
            td class="labels" {
                @for (k, v) in &n.config.labels { span class="chip" { (k) "=" (v) } " " }
                @for t in &n.config.taints { span class="chip taint" { (t.to_string()) } " " }
            }
        }
    }
}

#[derive(Deserialize)]
pub struct RangeQuery {
    range: Option<String>,
}

fn range_secs(r: &str) -> u64 {
    match r {
        "24h" => 86_400,
        "7d" => 7 * 86_400,
        _ => 3600,
    }
}

pub async fn detail(ctx: Ctx, Path(node): Path<String>, Query(q): Query<RangeQuery>) -> Response {
    match render_detail(&ctx, &node, q.range.as_deref().unwrap_or("1h")).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render_detail(ctx: &Ctx, id: &str, range: &str) -> anyhow::Result<Response> {
    let l = ctx.l;
    let s = status(ctx).await?;
    let n = s
        .nodes
        .iter()
        .find(|n| n.id == id || n.info.name == id)
        .ok_or_else(|| anyhow::anyhow!("no such node: {id}"))?
        .clone();
    let since = range_secs(range);
    let m = metrics(ctx, Some(n.id.clone()), since).await;
    let mv = m.nodes.iter().find(|x| x.id == n.id).cloned();
    let pw = power(ctx).await.into_iter().find(|p| p.id == n.id);
    let asleep = pw.as_ref().is_some_and(|p| p.asleep_since_unix.is_some());
    // Offered for any offline node that opted in: a machine that lost
    // power or slept without saying so wakes the same way.
    let wakeable = !n.online
        && pw
            .as_ref()
            .and_then(|p| p.report.as_ref())
            .is_some_and(|r| r.wake_on_lan);

    let top = html! {
        div class="page-head" {
            h1 { (n.info.name) " " (fmt::node_state(l, &n, asleep)) }
            @if ctx.me.is_admin() {
                div class="actions" {
                    @if n.config.cordoned {
                        button hx-post={"/nodes/" (n.id) "/uncordon"} hx-target="#flash" { (l.t("Uncordon", "차단 해제")) }
                    } @else {
                        button hx-post={"/nodes/" (n.id) "/cordon"} hx-target="#flash" { (l.t("Cordon", "차단")) }
                    }
                    button class="warn" hx-post={"/nodes/" (n.id) "/drain"} hx-target="#flash"
                        hx-confirm=(l.t("Drain this node? Its running tasks move to other nodes and it takes no new ones.",
                                        "이 노드를 비울까요? 실행 중인 태스크는 다른 노드로 옮기고 새 태스크를 받지 않습니다."))
                        { (l.t("Drain", "비우기(drain)")) }
                    @if n.online {
                        button hx-post={"/nodes/" (n.id) "/benchmark"} hx-target="#flash" { (l.t("Benchmark again", "벤치마크 다시")) }
                    }
                    @if wakeable {
                        button data-action="wake" hx-post={"/nodes/" (n.id) "/wake"} hx-target="#flash"
                            title=(l.t("Send this machine's Wake-on-LAN magic packet from a machine on its LAN",
                                       "같은 LAN에 있는 기기에서 이 기기의 Wake-on-LAN 매직 패킷을 보냅니다"))
                            { (l.t("Wake", "깨우기")) }
                    }
                    @if n.role != pumasi_proto::msg::Role::Owner {
                        button hx-post={"/nodes/" (n.id) "/trust"} hx-target="#flash"
                            title=(l.t("Run tier = \"trusted\" jobs here", "tier = \"trusted\" 작업을 여기서 실행"))
                            { (l.t("Trust host", "신뢰 호스트로")) }
                    }
                    @if !n.online {
                        button class="danger" hx-post={"/nodes/" (n.id) "/remove"} hx-target="#flash"
                            hx-confirm=(l.t("Remove this node from the cluster? It needs a new ticket to join again.",
                                            "이 노드를 클러스터에서 제거할까요? 다시 들어오려면 새 티켓이 필요합니다."))
                            { (l.t("Remove", "제거")) }
                    }
                }
            }
        }
        nav class="tabs" {
            @for (r, label) in [("1h", l.t("1 hour", "1시간")), ("24h", l.t("24 hours", "24시간")), ("7d", l.t("7 days", "7일"))] {
                a href={"/nodes/" (n.id) "?range=" (r)} class=[(r == range).then_some("active")] { (label) }
            }
        }
    };
    let live = html! {
        (top)
        (info(l, &n, pw.as_ref()))
        @if let Some(mv) = &mv { (charts(l, mv, since)) }
        @else { p class="empty" { (l.t("No metrics from this node (agents before protocol 10 send none).", "이 노드의 지표가 없습니다(프로토콜 10 이전 에이전트는 보내지 않습니다).")) } }
    };
    let bottom = html! {
        div class="cols" {
            (perf(l, &n))
            (labels(ctx, &n))
        }
    };
    Ok(ctx.page(
        &n.info.name,
        Nav::Nodes,
        html! {},
        Some(("10s", live)),
        bottom,
    ))
}

/// `stays awake while tasks run · says goodbye before sleeping`.
fn sleep_line(l: Lang, r: &pumasi_proto::power::PowerReport) -> String {
    let mut parts = vec![if r.stays_awake {
        l.t(
            "stays awake while tasks run",
            "태스크 실행 중에는 잠들지 않음",
        )
    } else {
        l.t(
            "may sleep while tasks run",
            "태스크 실행 중에도 잠들 수 있음",
        )
    }];
    if r.sleep_notice {
        parts.push(l.t("says so before it sleeps", "잠들기 전에 알림"));
    }
    if r.on_pause == pumasi_proto::power::OnPause::Requeue {
        parts.push(l.t(
            "its tasks move when it pauses",
            "일시 중지하면 태스크를 옮김",
        ));
    }
    parts.join(" · ")
}

fn info(l: Lang, n: &NodeView, pw: Option<&NodePowerView>) -> Markup {
    html! {
        section { dl class="facts" {
            dt { "ID" } dd { code { (n.id) } }
            dt { (l.t("System", "시스템")) } dd { (n.info.os) "/" (n.info.arch) ", " (n.info.cpus) " CPU, " (format_size(n.info.memory_bytes)) }
            dt { (l.t("Agent", "에이전트")) } dd { (n.info.agent_version) }
            dt { (l.t("Role", "역할")) } dd { (n.role.to_string()) " · " (n.account) }
            dt { (l.t("Executors", "실행기")) } dd { (n.executors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")) }
            dt { (l.t("Tasks", "태스크")) } dd { (n.running) " / " (n.info.slots) " · " (n.completed) " " (l.t("done", "완료")) }
            dt { (l.t("Last seen", "마지막 연결")) } dd { (fmt::ago(l, n.last_seen_unix)) }
            @if let Some(rtt) = n.rtt_us {
                dt { "RTT" } dd { (format!("{:.1} ms", rtt as f64 / 1000.0)) @if n.relayed { " (relay)" } }
            }
            dt { (l.t("Checks", "검증")) } dd { (n.checks_passed) " " (l.t("passed", "통과")) ", " (n.strikes) " " (l.t("failed", "실패")) }
            @if let Some(p) = &n.paused { dt { (l.t("Paused", "일시 중지")) } dd { (p) } }
            @if let Some(p) = pw {
                @if let Some(t) = p.asleep_since_unix {
                    dt { (l.t("Asleep", "잠듦")) } dd data-asleep="true" { (fmt::ago(l, t)) }
                }
                @if let Some(r) = &p.report {
                    dt { (l.t("Sleep", "절전")) } dd {
                        (sleep_line(l, r))
                        @for note in &r.notes { br; span class="dim" { (note) } }
                    }
                    dt { "Wake-on-LAN" } dd data-wol=(r.wake_on_lan) {
                        @if !r.wake_on_lan {
                            span class="dim" { (l.t(
                                "off (on that machine: pumasi service set -- --wake-on-lan)",
                                "꺼짐 (그 기기에서: pumasi service set -- --wake-on-lan)")) }
                        } @else if r.lan.is_empty() {
                            span class="dim" { (l.t("on, but the agent found no LAN network card", "켜짐, 하지만 LAN 네트워크 카드를 찾지 못함")) }
                        } @else {
                            @for (i, c) in r.lan.iter().enumerate() {
                                @if i > 0 { br; }
                                code { (format_mac(&c.mac)) } " " (c.name) " " span class="dim" { (c.cidr()) }
                                @if c.wireless { " " span class="badge warn" title=(l.t(
                                    "Most Wi-Fi cards do not wake from a magic packet: use a wired connection.",
                                    "대부분의 Wi-Fi 카드는 매직 패킷으로 깨어나지 않습니다. 유선 연결을 쓰세요.")) { "Wi-Fi" } }
                            }
                            @if !n.online && !p.can_wake {
                                br; span class="dim" { (l.t(
                                    "No machine of the cluster is online on its LAN now.",
                                    "지금 같은 LAN에 온라인인 클러스터 기기가 없습니다.")) }
                            }
                        }
                    }
                    @if let Some(t) = p.last_wake_unix {
                        dt { (l.t("Last wake", "마지막 깨우기")) } dd {
                            (fmt::ago(l, t))
                            @if let Some(by) = &p.last_wake_by { " · " (by) }
                        }
                    }
                }
            }
        } }
    }
}

fn charts(l: Lang, mv: &NodeMetricsView, since: u64) -> Markup {
    let to = mv
        .series
        .last()
        .map(|p| p.unix + mv.step_secs as u64)
        .unwrap_or_else(fmt::now_unix)
        .max(mv.latest_unix);
    let from = to.saturating_sub(since);
    let step = mv.step_secs as u64;
    let pts = |f: &dyn Fn(&pumasi_proto::monitor::MetricPoint) -> Option<f64>| -> Vec<(u64, f64)> {
        mv.series
            .iter()
            .filter_map(|p| f(p).map(|v| (p.unix, v)))
            .collect()
    };
    let mem_total = mv.series.iter().map(|p| p.memory_total).max().unwrap_or(0) as f64;
    let has_gpu = mv.series.iter().any(|p| p.gpu_percent.is_some());
    let has_temp = mv.series.iter().any(|p| p.temp_c.is_some());
    let pct = |v: f64| format!("{v:.0}%");
    let bytes = |v: f64| format_size(v as u64);
    let rate = |v: f64| format!("{}/s", format_size(v as u64));
    let mut cpu = vec![Series {
        label: "CPU".into(),
        points: pts(&|p| Some(p.cpu_percent as f64)),
    }];
    if has_gpu {
        cpu.push(Series {
            label: "GPU".into(),
            points: pts(&|p| p.gpu_percent.map(|v| v as f64)),
        });
    }
    html! {
        div class="charts" {
            (line_chart(l.t("CPU use", "CPU 사용률"), &cpu, Some(100.0), step, (from, to), &pct))
            (line_chart(l.t("Memory", "메모리"), &[Series { label: l.t("used", "사용").into(), points: pts(&|p| Some(p.memory_used as f64)) }],
                (mem_total > 0.0).then_some(mem_total), step, (from, to), &bytes))
            (line_chart(l.t("Network", "네트워크"), &[
                Series { label: l.t("in", "수신").into(), points: pts(&|p| Some(p.net_rx_bps as f64)) },
                Series { label: l.t("out", "송신").into(), points: pts(&|p| Some(p.net_tx_bps as f64)) },
            ], None, step, (from, to), &rate))
            (line_chart(l.t("Load and tasks", "부하와 태스크"), &[
                Series { label: l.t("load (1 min)", "부하(1분)").into(), points: pts(&|p| Some(p.load1 as f64)) },
                Series { label: l.t("tasks", "태스크").into(), points: pts(&|p| Some(p.running as f64)) },
            ], None, step, (from, to), &|v| format!("{v:.1}")))
            @if has_temp {
                (line_chart(l.t("Temperature (hottest)", "온도(최고)"), &[Series { label: "°C".into(), points: pts(&|p| p.temp_c.map(|v| v as f64)) }],
                    None, step, (from, to), &|v| format!("{v:.0} °C")))
            }
            @if has_gpu {
                (line_chart(l.t("GPU memory", "GPU 메모리"), &[Series { label: "VRAM".into(), points: pts(&|p| Some(p.gpu_mem_used_mib as f64 * 1048576.0)) }],
                    None, step, (from, to), &bytes))
            }
        }
    }
}

fn perf(l: Lang, n: &NodeView) -> Markup {
    html! {
        section {
            h2 { (l.t("Benchmark", "벤치마크")) }
            @if let Some(p) = &n.perf {
                dl class="facts" {
                    dt { (l.t("Measured", "측정")) } dd { (fmt::time(p.measured_unix)) }
                    dt { "CPU" } dd { (p.cpu.model) }
                    dt { (l.t("CPU score", "CPU 점수")) } dd {
                        (l.t("billed", "과금")) " " strong { (n.cpu_score) } " · "
                        (l.t("single", "단일")) " " (p.cpu.single) " · "
                        (l.t("all", "전체")) " " (p.cpu.multi) " (" (p.cpu.threads) ")"
                    }
                    @if let Some(note) = &n.perf_note { dt {} dd class="dim" { (note) } }
                    dt { (l.t("Memory bandwidth", "메모리 대역폭")) } dd { (p.memory.copy_mb_s) " MB/s" }
                    @if let Some(d) = &p.disk {
                        dt { (l.t("Disk", "디스크")) } dd {
                            (format!("{:?}", d.class).to_lowercase()) " · "
                            (l.t("read", "읽기")) " " (d.seq_read_mb_s) " MB/s · "
                            (l.t("write", "쓰기")) " " (d.seq_write_mb_s) " MB/s · "
                            (d.rand_read_iops) " IOPS"
                        }
                    }
                    @for note in &p.notes { dt {} dd class="dim" { (note) } }
                }
            } @else {
                p class="empty" { (l.t("Not benchmarked yet.", "아직 벤치마크하지 않았습니다.")) }
            }
            @if !n.gpus.is_empty() {
                h3 { "GPU" }
                @for (en, ko) in n.gpu_hints() { p class="dim" { (l.t(en, ko)) } }
                ul class="plain" {
                    @for g in &n.gpus {
                        li {
                            strong { "#" (g.info.index) " " (g.info.name) } " · "
                            (format_size(g.info.vram_mib * 1024 * 1024)) " · "
                            (g.billed_gflops) " GFLOPS"
                            @if let Some(t) = &g.busy { " · " span class="badge info" { (l.t("busy", "사용 중")) } " " a href={"/jobs/" (t.job)} { code { (t.job) } } }
                            @if let Some(note) = &g.note { br; span class="dim" { (note) } }
                        }
                    }
                }
            }
            @if !n.webgpu.is_empty() {
                h3 { "WebGPU" }
                p class="dim" { (l.t("Wasm jobs with gpus = 1 run compute shaders here (experimental).", "gpus = 1 인 wasm 작업이 여기서 컴퓨트 셰이더를 실행합니다(실험).")) }
                ul class="plain" {
                    @for w in &n.webgpu {
                        li {
                            strong { "#" (w.adapter.index) " " (w.adapter.name) } " · "
                            (w.adapter.backend) " · " (w.adapter.device_type) " · "
                            (w.billed_gflops) " GFLOPS"
                            @if let Some(g) = w.adapter.gpu { " · GPU #" (g) }
                            @if let Some(t) = &w.busy { " · " span class="badge info" { (l.t("busy", "사용 중")) } " " a href={"/jobs/" (t.job)} { code { (t.job) } } }
                            @if let Some(note) = &w.note { br; span class="dim" { (note) } }
                        }
                    }
                }
            }
        }
    }
}

fn labels(ctx: &Ctx, n: &NodeView) -> Markup {
    let l = ctx.l;
    let admin = ctx.me.is_admin();
    html! {
        section {
            h2 { (l.t("Name, labels and taints", "이름, 라벨, 테인트")) }
            p class="labels" {
                @for (k, vs) in &n.labels {
                    @let user = n.config.labels.contains_key(k);
                    @for v in vs {
                        span class={"chip" @if !user { " builtin" }} { (k) "=" (v)
                            @if user && admin {
                                button class="chip-x" title=(l.t("Remove", "제거"))
                                    hx-post={"/nodes/" (n.id) "/unlabel"} hx-vals=(serde_json::json!({"key": k}).to_string()) hx-target="#flash" { "×" }
                            }
                        }
                        " "
                    }
                }
                @for t in &n.config.taints {
                    span class="chip taint" { (t.to_string())
                        @if admin {
                            button class="chip-x" title=(l.t("Remove", "제거"))
                                hx-post={"/nodes/" (n.id) "/untaint"} hx-vals=(serde_json::json!({"key": t.key}).to_string()) hx-target="#flash" { "×" }
                        }
                    }
                    " "
                }
            }
            @if admin {
                form class="inline" hx-post={"/nodes/" (n.id) "/rename"} hx-target="#flash" {
                    input name="value" value=(n.info.name) maxlength="64" size="16"
                        aria-label=(l.t("Node name", "노드 이름"))
                        title=(l.t("Leave empty to go back to the name the node's agent reports.",
                                   "비우면 노드 에이전트가 알려 주는 이름으로 돌아갑니다."));
                    button type="submit" { (l.t("Rename", "이름 변경")) }
                }
                form class="inline" hx-post={"/nodes/" (n.id) "/label"} hx-target="#flash" {
                    input name="key" placeholder=(l.t("key", "키")) required size="10";
                    input name="value" placeholder=(l.t("value", "값")) size="10";
                    button type="submit" { (l.t("Add label", "라벨 추가")) }
                }
                form class="inline" hx-post={"/nodes/" (n.id) "/taint"} hx-target="#flash" {
                    input name="key" placeholder="key[=value]" required size="16";
                    button type="submit" { (l.t("Add taint", "테인트 추가")) }
                }
            }
        }
    }
}

#[derive(Deserialize, Default)]
pub struct EditForm {
    key: Option<String>,
    value: Option<String>,
}

pub async fn action(
    ctx: Ctx,
    headers: HeaderMap,
    Path((node, action)): Path<(String, String)>,
    Form(f): Form<EditForm>,
) -> Response {
    match do_action(&ctx, &node, &action, f).await {
        // A new name shows outside the live region too (title, labels).
        Ok(msg) if action == "rename" => {
            let mut r = ctx.done(msg);
            r.headers_mut()
                .insert("hx-refresh", axum::http::HeaderValue::from_static("true"));
            r
        }
        Ok(msg) => ctx.done(msg),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}

async fn do_action(ctx: &Ctx, node: &str, action: &str, f: EditForm) -> anyhow::Result<String> {
    let l = ctx.l;
    let edit = |e: NodeEdit| AdminOp::EditNode {
        node: node.to_string(),
        edit: Box::new(e),
    };
    let key = || -> anyhow::Result<String> {
        let k = f.key.clone().unwrap_or_default().trim().to_string();
        anyhow::ensure!(!k.is_empty(), "a key is needed");
        Ok(k)
    };
    match action {
        "cordon" | "uncordon" => {
            ctx.call(edit(NodeEdit {
                cordon: Some(action == "cordon"),
                ..Default::default()
            }))
            .await?;
            Ok(if action == "cordon" {
                l.t(
                    "Cordoned: no new tasks; running ones finish.",
                    "차단했습니다. 새 태스크를 받지 않고 실행 중인 것은 마칩니다.",
                )
                .into()
            } else {
                l.t("Uncordoned.", "차단을 풀었습니다.").into()
            })
        }
        "wake" => match ctx.call(AdminOp::WakeNode { node: node.into() }).await? {
            AdminResponse::Applied(lines) => Ok(format!(
                "{} {}",
                l.t("Magic packet sent:", "매직 패킷을 보냈습니다:"),
                lines.join("; ")
            )),
            _ => anyhow::bail!("unexpected response"),
        },
        "drain" => match ctx.call(AdminOp::Drain { node: node.into() }).await? {
            AdminResponse::Drained { moved } => Ok(format!(
                "{} {moved}",
                l.t(
                    "Drained. Tasks moved to other nodes:",
                    "비웠습니다. 다른 노드로 옮긴 태스크:"
                )
            )),
            _ => Ok(l.t("Drained.", "비웠습니다.").into()),
        },
        "benchmark" => {
            ctx.call(AdminOp::Benchmark { id: node.into() }).await?;
            Ok(l.t(
                "Asked the node to benchmark itself again.",
                "노드에 벤치마크를 다시 요청했습니다.",
            )
            .into())
        }
        "remove" => {
            ctx.call(AdminOp::RemoveNode { id: node.into() }).await?;
            Ok(l.t("Removed.", "제거했습니다.").into())
        }
        "trust" | "untrust" => {
            ctx.call(AdminOp::TrustNode {
                node: node.into(),
                trusted: action == "trust",
            })
            .await?;
            Ok(if action == "trust" {
                l.t(
                    "It now runs tier = \"trusted\" jobs.",
                    "이제 tier = \"trusted\" 작업을 실행합니다.",
                )
            } else {
                l.t(
                    "It no longer runs tier = \"trusted\" jobs.",
                    "더 이상 tier = \"trusted\" 작업을 실행하지 않습니다.",
                )
            }
            .into())
        }
        // `node` is the measurement's id here.
        "trust-measurement" | "untrust-measurement" => {
            let trusted = action == "trust-measurement";
            ctx.call(AdminOp::TrustMeasurement {
                id: node.into(),
                trusted,
                note: String::new(),
            })
            .await?;
            Ok(if trusted {
                l.t("Measurement trusted.", "측정값을 신뢰합니다.")
            } else {
                l.t(
                    "Measurement no longer trusted.",
                    "측정값 신뢰를 해제했습니다.",
                )
            }
            .into())
        }
        "rename" => {
            let name = f.value.unwrap_or_default().trim().to_string();
            let name = (!name.is_empty()).then_some(name);
            ctx.call(AdminOp::RenameNode {
                node: node.to_string(),
                name: name.clone(),
            })
            .await?;
            Ok(match name {
                Some(n) => format!("{} {n}", l.t("Renamed to", "이름을 바꿨습니다:")),
                None => l
                    .t(
                        "It goes by the name its agent reports again.",
                        "에이전트가 알려 주는 이름으로 되돌렸습니다.",
                    )
                    .into(),
            })
        }
        "label" => {
            let k = key()?;
            let v = f.value.unwrap_or_default().trim().to_string();
            ctx.call(edit(NodeEdit {
                set_labels: [(k.clone(), v)].into(),
                ..Default::default()
            }))
            .await?;
            Ok(format!("{} {k}", l.t("Set label", "라벨 설정:")))
        }
        "unlabel" => {
            let k = key()?;
            ctx.call(edit(NodeEdit {
                remove_labels: vec![k.clone()],
                ..Default::default()
            }))
            .await?;
            Ok(format!("{} {k}", l.t("Removed label", "라벨 제거:")))
        }
        "taint" => {
            let t = key()?.parse()?;
            ctx.call(edit(NodeEdit {
                add_taints: vec![t],
                ..Default::default()
            }))
            .await?;
            Ok(l.t("Added the taint.", "테인트를 추가했습니다.").into())
        }
        "untaint" => {
            let k = key()?;
            ctx.call(edit(NodeEdit {
                remove_taints: vec![k.clone()],
                ..Default::default()
            }))
            .await?;
            Ok(format!("{} {k}", l.t("Removed taint", "테인트 제거:")))
        }
        _ => anyhow::bail!("no such action: {action}"),
    }
}
