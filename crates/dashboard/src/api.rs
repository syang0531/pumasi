//! The JSON API (`/api/v1`), for scripts and other tools. It answers with
//! the same views the CLI gets (job ids as hex strings), takes a signed-in
//! browser session or `Authorization: Bearer <ticket>`, and the control
//! plane applies the ticket's role. `POST /api/v1/op` takes any admin call
//! as JSON. Described in `assets/openapi.json`.

// Handlers return early with a finished response as the error.
#![allow(clippy::result_large_err)]

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use pumasi_proto::manage::AccessRole;
use pumasi_proto::{
    job::{JobId, parse_duration_ms},
    manage::{NodeEdit, Selector},
    msg::{AdminOp, AdminResponse, Invite},
    receipt::ReceiptRow,
    release::{CanarySpec, DEFAULT_CANARY_SOAK_SECS},
    ui::NodeSettings,
};
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::{St, accounts, auth::Api, pages};

pub const OPENAPI: &str = include_str!("../assets/openapi.json");

/// The API's routes; `uploads` is the body limit of job submissions.
pub fn routes(uploads: DefaultBodyLimit) -> Router<St> {
    let op_limit = DefaultBodyLimit::max(crate::OP_BODY_LIMIT);
    // Named here so that every route below stays on one line, which is
    // how the test finds them.
    let policy = get(account_policy).patch(patch_account_policy);
    Router::new()
        .route("/whoami", get(whoami))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/events", get(events))
        .route("/alerts", get(alerts))
        .route("/alerts/silences", post(silence))
        .route("/alerts/silences/{id}", delete(unsilence))
        .route("/webhooks/{name}/test", post(test_webhook))
        .route("/net", get(net))
        .route("/jobs", post(submit).layer(uploads))
        .route("/jobs/{id}", get(job))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/receipts", get(receipts))
        .route("/jobs/{id}/outputs.zip", get(outputs_zip))
        .route("/jobs/{id}/tasks/{index}/logs", get(logs))
        .route("/nodes/{node}/rename", post(rename_node))
        .route(
            "/nodes/{node}/settings",
            get(node_settings).put(set_node_settings),
        )
        .route("/nodes/{node}/{action}", post(node_action))
        .route("/upgrade", get(upgrade_status).post(upgrade_latest))
        .route("/upgrade/canary", get(canary))
        .route("/upgrade/canary/promote", post(promote_canary))
        .route("/sessions", get(sessions))
        .route("/sessions/{id}", delete(revoke_session))
        .route("/apply", post(apply))
        .route("/accounts", get(accounts))
        .route("/accounts/{name}/policy", policy)
        .route("/ledger", get(ledger))
        .route("/datasets", get(datasets))
        .route("/storage", get(storage))
        .route("/prices", get(prices))
        .route("/confidential", get(confidential))
        .route("/invites", post(invite))
        .route("/op", post(op).layer(op_limit))
        .route("/audit", get(audit))
        .route("/audit/verify", get(audit_verify))
}

pub async fn openapi() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        OPENAPI,
    )
        .into_response()
}

pub fn error(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({ "error": msg }))).into_response()
}

fn fail(e: anyhow::Error) -> Response {
    let msg = format!("{e:#}");
    error(pages::status_for(&msg), &msg)
}

async fn call(st: &St, a: &Api, op: AdminOp) -> Result<AdminResponse, Response> {
    st.backend.call(a.0.secret, op).await.map_err(fail)
}

/// Answers with the response's payload, without the enum's tag.
fn payload(r: AdminResponse) -> Response {
    let v = match serde_json::to_value(&r) {
        Ok(serde_json::Value::Object(m)) if m.len() == 1 => m.into_iter().next().unwrap().1,
        Ok(serde_json::Value::String(s)) if s == "Ok" => json!({ "ok": true }),
        Ok(v) => v,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    Json(v).into_response()
}

async fn simple(st: St, a: Api, op: AdminOp) -> Response {
    match call(&st, &a, op).await {
        Ok(r) => payload(r),
        Err(r) => r,
    }
}

fn job_id(s: &str) -> Result<JobId, Response> {
    s.parse()
        .map_err(|_| error(StatusCode::NOT_FOUND, &format!("no such job: {s}")))
}

type S = axum::extract::State<St>;

/// The ticket's name and role, and for a submitter what its account may
/// do (1.4): the job policy its submissions are held to, its quota and
/// what it uses now. Admin and viewer tickets are not held to a policy.
async fn whoami(axum::extract::State(st): S, a: Api) -> Response {
    let (name, role) = match call(&st, &a, AdminOp::WhoAmI).await {
        Ok(AdminResponse::WhoAmI { name, role }) => (name, role),
        Ok(_) => return error(StatusCode::BAD_GATEWAY, "unexpected response"),
        Err(r) => return r,
    };
    let mut out = json!({
        "name": name,
        "role": role,
        "account": null,
        "policy": null,
        "quota": null,
        "usage": null,
    });
    if let AccessRole::Submitter { account } = &role {
        match accounts::get(&st, a.0.secret, &role, account).await {
            Ok(Some(v)) => {
                out["account"] = json!(v.account);
                out["policy"] = accounts::policy_json(&v.policy);
                out["quota"] = accounts::quota_json(&v.quota);
                out["usage"] = accounts::usage_json(&v);
            }
            Ok(None) => {}
            Err(e) => return fail(e),
        }
    }
    Json(out).into_response()
}

/// The cluster's status. Each node also carries `power` (1.5): whether
/// it is asleep, what its agent does about sleep, and whether the cluster
/// can wake it. Missing for a control plane older than 1.5.
async fn status(axum::extract::State(st): S, a: Api) -> Response {
    let status = match call(&st, &a, AdminOp::Status).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let mut v = match serde_json::to_value(&status) {
        Ok(serde_json::Value::Object(m)) if m.len() == 1 => m.into_iter().next().unwrap().1,
        Ok(v) => v,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    if let Ok(AdminResponse::NodePower(power)) = call(&st, &a, AdminOp::NodePower).await
        && let Some(nodes) = v.get_mut("nodes").and_then(|n| n.as_array_mut())
    {
        for n in nodes {
            let id = n.get("id").and_then(|i| i.as_str()).unwrap_or_default();
            if let Some(p) = power.iter().find(|p| p.id == id)
                && let Some(n) = n.as_object_mut()
            {
                n.insert("power".into(), json!(p));
            }
        }
    }
    Json(v).into_response()
}

#[derive(Deserialize)]
struct MetricsQuery {
    node: Option<String>,
    #[serde(default)]
    since: u64,
}

async fn metrics(axum::extract::State(st): S, a: Api, Query(q): Query<MetricsQuery>) -> Response {
    simple(
        st,
        a,
        AdminOp::Metrics {
            node: q.node,
            since_secs: q.since,
        },
    )
    .await
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after: u64,
    #[serde(default)]
    since: u64,
    limit: Option<u32>,
}

async fn events(axum::extract::State(st): S, a: Api, Query(q): Query<EventsQuery>) -> Response {
    simple(
        st,
        a,
        AdminOp::Events {
            after: q.after,
            since_unix: q.since,
            limit: q.limit.unwrap_or(100).min(10_000),
        },
    )
    .await
}

/// The alerts view, with the 1.6 tuning merged in when the control
/// plane has it.
async fn alerts(axum::extract::State(st): S, a: Api) -> Response {
    let mut v = match call(&st, &a, AdminOp::Alerts).await {
        Ok(AdminResponse::Alerts(v)) => match serde_json::to_value(&*v) {
            Ok(v) => v,
            Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        },
        Ok(r) => return payload(r),
        Err(r) => return r,
    };
    if let Ok(AdminResponse::AlertTuning(t)) = call(&st, &a, AdminOp::AlertTuning).await
        && let (Some(obj), Ok(serde_json::Value::Object(extra))) =
            (v.as_object_mut(), serde_json::to_value(&*t))
    {
        obj.extend(extra);
    }
    Json(v).into_response()
}

#[derive(Deserialize)]
struct SilenceBody {
    rule: Option<String>,
    on: Option<String>,
    #[serde(rename = "for")]
    duration: Option<String>,
    comment: Option<String>,
}

async fn silence(axum::extract::State(st): S, a: Api, Json(b): Json<SilenceBody>) -> Response {
    match pages::events::silence_spec(b.rule, b.on, b.duration, b.comment) {
        Ok(spec) => simple(st, a, AdminOp::Silence(Box::new(spec))).await,
        Err(e) => error(StatusCode::BAD_REQUEST, &format!("{e:#}")),
    }
}

async fn unsilence(axum::extract::State(st): S, a: Api, Path(id): Path<String>) -> Response {
    simple(st, a, AdminOp::Unsilence { id }).await
}

async fn test_webhook(axum::extract::State(st): S, a: Api, Path(name): Path<String>) -> Response {
    simple(st, a, AdminOp::TestWebhook { name }).await
}

async fn net(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Net).await
}

/// The longest `?wait=` (phase 15): under the 100 seconds Cloudflare
/// gives a response to start.
pub const MAX_WAIT: Duration = Duration::from_secs(90);

#[derive(Deserialize)]
struct JobQuery {
    /// Long poll: answer when the job finishes, or after this long
    /// (`30s`, `2m`; a bare number is seconds), at most [`MAX_WAIT`].
    wait: Option<String>,
}

async fn job(
    axum::extract::State(st): S,
    a: Api,
    Path(id): Path<String>,
    Query(q): Query<JobQuery>,
) -> Response {
    let id = match job_id(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    let wait = match q.wait.as_deref().map(pumasi_proto::job::parse_duration_ms) {
        None => Duration::ZERO,
        Some(Ok(ms)) => Duration::from_millis(ms).min(MAX_WAIT),
        Some(Err(e)) => return error(StatusCode::BAD_REQUEST, &format!("wait: {e:#}")),
    };
    // Each open long poll holds a task here and calls the control plane:
    // a few per ticket at once.
    let _slot = if wait.is_zero() {
        None
    } else {
        match st.waits.enter(a.0.secret) {
            Some(s) => Some(s),
            None => {
                return crate::limit::slow_down(
                    Duration::from_secs(1),
                    "too many ?wait= requests open with this ticket",
                );
            }
        }
    };
    let deadline = tokio::time::Instant::now() + wait;
    let mut pause = Duration::from_millis(250);
    loop {
        let r = match call(
            &st,
            &a,
            AdminOp::Job {
                id,
                with_output: false,
            },
        )
        .await
        {
            Ok(r) => r,
            Err(r) => return r,
        };
        let done = match &r {
            AdminResponse::Job(j) => j.summary.state.is_terminal(),
            _ => true,
        };
        let now = tokio::time::Instant::now();
        if done || now >= deadline {
            return match r {
                AdminResponse::Job(j) => job_json(&st, &a, id, &j).await,
                r => payload(r),
            };
        }
        tokio::time::sleep(pause.min(deadline - now)).await;
        pause = (pause * 2).min(Duration::from_secs(2));
    }
}

/// A job view with, on each task, the name of its node and how long each
/// part of the run took (1.4). Those come from a second call that control
/// planes older than 1.4 do not answer; the tasks then go without them.
async fn job_json(st: &St, a: &Api, id: JobId, job: &pumasi_proto::msg::JobView) -> Response {
    let mut v = match serde_json::to_value(job) {
        Ok(v) => v,
        Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    if let Ok(AdminResponse::TaskDetails(details)) = st
        .backend
        .call(a.0.secret, AdminOp::TaskDetails { id })
        .await
        && let Some(tasks) = v.get_mut("tasks").and_then(|t| t.as_array_mut())
    {
        for d in details {
            let Some(t) = tasks
                .get_mut(d.index as usize)
                .and_then(|t| t.as_object_mut())
            else {
                continue;
            };
            t.insert("node_name".into(), json!(d.node_name));
            t.insert("queued_ms".into(), json!(d.queued_ms));
            t.insert("fetch_ms".into(), json!(d.fetch_ms));
            t.insert("pull_ms".into(), json!(d.pull_ms));
            t.insert("run_ms".into(), json!(d.run_ms));
            t.insert("upload_ms".into(), json!(d.upload_ms));
        }
    }
    // What each task of an `image` job ran as (1.5): docker or microvm.
    if let Ok(AdminResponse::TaskExecutors(ran)) = st
        .backend
        .call(a.0.secret, AdminOp::TaskExecutors { id })
        .await
        && let Some(tasks) = v.get_mut("tasks").and_then(|t| t.as_array_mut())
    {
        for (t, e) in tasks.iter_mut().zip(ran) {
            if let Some(t) = t.as_object_mut() {
                t.insert("ran_as".into(), json!(e.map(|e| e.to_string())));
            }
        }
    }
    Json(v).into_response()
}

#[derive(Deserialize)]
struct SubmitQuery {
    /// `1` or `true`: check the job without creating it.
    validate: Option<String>,
}

async fn submit(
    axum::extract::State(st): S,
    a: Api,
    Query(q): Query<SubmitQuery>,
    form: Multipart,
) -> Response {
    let validate = match q.validate.as_deref() {
        None | Some("0" | "false") => false,
        Some("1" | "true" | "") => true,
        Some(other) => {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("validate is 1 or 0, not {other:?}"),
            );
        }
    };
    if validate {
        let res = async {
            let up = crate::uploads::receive(form, "job").await?;
            pages::jobs::check_upload(&st, a.0.secret, up).await
        }
        .await;
        return match res {
            Ok(c) => Json(json!({
                "valid": true,
                "account": c.account,
                "tasks": c.tasks,
                "ready_nodes": c.ready_nodes,
                "waiting": c.waiting,
            }))
            .into_response(),
            Err(e) => fail(e),
        };
    }
    let res = async {
        let up = crate::uploads::receive(form, "job").await?;
        pages::jobs::submit_upload(&st, a.0.secret, up).await
    }
    .await;
    match res {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(e) => fail(e),
    }
}

async fn cancel(axum::extract::State(st): S, a: Api, Path(id): Path<String>) -> Response {
    match job_id(&id) {
        Ok(id) => simple(st, a, AdminOp::Cancel { id }).await,
        Err(r) => r,
    }
}

async fn receipts(axum::extract::State(st): S, a: Api, Path(id): Path<String>) -> Response {
    let id = match job_id(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match call(&st, &a, AdminOp::Receipts { id }).await {
        Ok(AdminResponse::Receipts(r)) => {
            Json(r.iter().map(ReceiptRow::from).collect::<Vec<_>>()).into_response()
        }
        Ok(_) => error(StatusCode::BAD_GATEWAY, "unexpected response"),
        Err(r) => r,
    }
}

async fn outputs_zip(axum::extract::State(st): S, a: Api, Path(id): Path<String>) -> Response {
    match pages::jobs::build_outputs_zip(&st, a.0.secret, &id).await {
        Ok((dir, zip, name)) => pages::jobs::zip_response(dir, &zip, &name).await,
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default)]
    attempt: u32,
    #[serde(default)]
    stdout_from: u64,
    #[serde(default)]
    stderr_from: u64,
}

/// A task's output from byte offsets, as text (invalid UTF-8 replaced);
/// poll with the returned offsets, as `pumasi logs -f` does.
async fn logs(
    axum::extract::State(st): S,
    a: Api,
    Path((id, index)): Path<(String, u32)>,
    Query(q): Query<LogsQuery>,
) -> Response {
    let job = match job_id(&id) {
        Ok(i) => i,
        Err(r) => return r,
    };
    match call(
        &st,
        &a,
        AdminOp::Logs {
            job,
            index,
            attempt: q.attempt,
            stdout_from: q.stdout_from,
            stderr_from: q.stderr_from,
        },
    )
    .await
    {
        Ok(AdminResponse::Logs(c)) => Json(json!({
            "attempt": c.attempt,
            "stdout_from": c.stdout_from,
            "stdout": String::from_utf8_lossy(&c.stdout),
            "stdout_next": c.stdout_from + c.stdout.len() as u64,
            "stderr_from": c.stderr_from,
            "stderr": String::from_utf8_lossy(&c.stderr),
            "stderr_next": c.stderr_from + c.stderr.len() as u64,
            "state": c.state,
            "done": c.done,
            "live": c.live,
        }))
        .into_response(),
        Ok(_) => error(StatusCode::BAD_GATEWAY, "unexpected response"),
        Err(r) => r,
    }
}

async fn node_action(
    axum::extract::State(st): S,
    a: Api,
    Path((node, action)): Path<(String, String)>,
) -> Response {
    let cordon = |c: bool| AdminOp::EditNode {
        node: node.clone(),
        edit: Box::new(NodeEdit {
            cordon: Some(c),
            ..Default::default()
        }),
    };
    let op = match action.as_str() {
        "drain" => AdminOp::Drain { node: node.clone() },
        "cordon" => cordon(true),
        "uncordon" => cordon(false),
        "benchmark" => AdminOp::Benchmark { id: node.clone() },
        "wake" => AdminOp::WakeNode { node: node.clone() },
        "trust" | "untrust" => AdminOp::TrustNode {
            node: node.clone(),
            trusted: action == "trust",
        },
        _ => {
            return error(
                StatusCode::NOT_FOUND,
                "actions: drain, cordon, uncordon, benchmark, wake, trust, untrust",
            );
        }
    };
    simple(st, a, op).await
}

#[derive(Deserialize)]
struct RenameBody {
    /// The new name; null or missing goes back to the agent's name.
    name: Option<String>,
}

async fn rename_node(
    axum::extract::State(st): S,
    a: Api,
    Path(node): Path<String>,
    Json(body): Json<RenameBody>,
) -> Response {
    simple(
        st,
        a,
        AdminOp::RenameNode {
            node,
            name: body.name.filter(|n| !n.trim().is_empty()),
        },
    )
    .await
}

#[derive(Deserialize)]
struct UpgradeQuery {
    #[serde(default)]
    check: bool,
}

async fn upgrade_status(
    axum::extract::State(st): S,
    a: Api,
    Query(q): Query<UpgradeQuery>,
) -> Response {
    simple(st, a, AdminOp::LatestRelease { check: q.check }).await
}

#[derive(Deserialize, Default)]
struct UpgradeBody {
    version: Option<String>,
    #[serde(default)]
    force: bool,
    /// Canary nodes (1.7): names, comma-separated, or a label selector.
    canary: Option<String>,
    /// How long the canaries run before the rest follow (`30m`).
    canary_soak: Option<String>,
    #[serde(default)]
    canary_tasks: u32,
    #[serde(default)]
    canary_hold: bool,
}

async fn upgrade_latest(
    axum::extract::State(st): S,
    a: Api,
    body: Option<Json<UpgradeBody>>,
) -> Response {
    let b = body.map(|b| b.0).unwrap_or_default();
    let canary = match b.canary.as_deref().map(str::trim).filter(|c| !c.is_empty()) {
        None => None,
        Some(nodes) => {
            let spec = (|| -> anyhow::Result<CanarySpec> {
                let spec = CanarySpec {
                    nodes: Selector::nodes(nodes)?,
                    soak_secs: match b.canary_soak.as_deref() {
                        Some(s) => parse_duration_ms(s)? / 1000,
                        None => DEFAULT_CANARY_SOAK_SECS,
                    },
                    min_tasks: b.canary_tasks,
                    hold: b.canary_hold,
                };
                spec.validate()?;
                Ok(spec)
            })();
            match spec {
                Ok(s) => Some(s),
                Err(e) => return error(StatusCode::BAD_REQUEST, &format!("{e:#}")),
            }
        }
    };
    let op = match canary {
        Some(c) => AdminOp::CanaryUpgradeToLatest {
            version: b.version,
            force: b.force,
            canary: Box::new(c),
        },
        None => AdminOp::UpgradeToLatest {
            version: b.version,
            force: b.force,
        },
    };
    simple(st, a, op).await
}

/// The current rollout's canary, or `null` (1.7).
async fn canary(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Canary).await
}

#[derive(Deserialize, Default)]
struct PromoteBody {
    #[serde(default)]
    nodes: Vec<String>,
}

/// Upgrades the nodes named next, or every node when none is (1.7).
async fn promote_canary(
    axum::extract::State(st): S,
    a: Api,
    body: Option<Json<PromoteBody>>,
) -> Response {
    let nodes = body.map(|b| b.0.nodes).unwrap_or_default();
    simple(st, a, AdminOp::PromoteCanary { nodes }).await
}

/// Browsers signed in (1.7): every one for an admin, else the caller's.
async fn sessions(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Sessions).await
}

async fn revoke_session(axum::extract::State(st): S, a: Api, Path(id): Path<String>) -> Response {
    let r = simple(st.clone(), a, AdminOp::RevokeSession { id: id.clone() }).await;
    if r.status().is_success() {
        st.sessions.forget_id(&id);
    }
    r
}

/// A node's availability settings (1.7).
async fn node_settings(axum::extract::State(st): S, a: Api, Path(node): Path<String>) -> Response {
    simple(st, a, AdminOp::NodeSettings { node }).await
}

/// Changes a node's availability settings: the agent writes them to its
/// options file and restarts with them once its tasks end (1.7).
async fn set_node_settings(
    axum::extract::State(st): S,
    a: Api,
    Path(node): Path<String>,
    Json(settings): Json<NodeSettings>,
) -> Response {
    if let Err(e) = settings.validate() {
        return error(StatusCode::BAD_REQUEST, &format!("{e:#}"));
    }
    simple(
        st,
        a,
        AdminOp::SetNodeSettings {
            node,
            settings: Box::new(settings),
        },
    )
    .await
}

#[derive(Deserialize)]
struct ApplyBody {
    /// A `cluster.toml`, without schedules or `url_env`.
    file: String,
    #[serde(default)]
    dry_run: bool,
}

/// Makes the cluster match a `cluster.toml` (1.7); with `dry_run`, only
/// says what would change.
async fn apply(axum::extract::State(st): S, a: Api, Json(b): Json<ApplyBody>) -> Response {
    match pages::settings::apply_spec(&b.file, b.dry_run) {
        Ok(spec) => simple(st, a, AdminOp::Apply(Box::new(spec))).await,
        Err(e) => error(StatusCode::BAD_REQUEST, &format!("{e:#}")),
    }
}

async fn accounts(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Accounts).await
}

/// An account's job policy, quota and use (1.4). Admins and viewers may
/// read any account, a submitter its own.
async fn account_policy(axum::extract::State(st): S, a: Api, Path(name): Path<String>) -> Response {
    match accounts::get(&st, a.0.secret, &a.0.role, &name).await {
        Ok(Some(v)) => Json(accounts::view_json(&v)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, &format!("no such account: {name}")),
        Err(e) => fail(e),
    }
}

/// Changes the fields the body names and leaves the others (1.4). Admin
/// only: the control plane refuses the change from other tickets.
async fn patch_account_policy(
    axum::extract::State(st): S,
    a: Api,
    Path(name): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let res = async {
        pumasi_proto::economy::validate_account(&name)?;
        let (policy, quota) = accounts::parse_patch(&name, &body)?;
        accounts::patch(&st, a.0.secret, &a.0.role, &name, &policy, &quota).await
    }
    .await;
    match res {
        Ok(v) => Json(accounts::view_json(&v)).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct LedgerQuery {
    account: Option<String>,
    limit: Option<u32>,
}

async fn ledger(axum::extract::State(st): S, a: Api, Query(q): Query<LedgerQuery>) -> Response {
    simple(
        st,
        a,
        AdminOp::Ledger {
            account: q.account,
            limit: q.limit.unwrap_or(100),
        },
    )
    .await
}

#[derive(Deserialize)]
struct DatasetsQuery {
    name: Option<String>,
}

async fn datasets(axum::extract::State(st): S, a: Api, Query(q): Query<DatasetsQuery>) -> Response {
    simple(st, a, AdminOp::StoreList { name: q.name }).await
}

async fn storage(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::StoreUsage).await
}

async fn prices(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::GetPolicy).await
}

async fn confidential(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Confidential).await
}

#[derive(Deserialize)]
struct InviteBody {
    /// Without an account: a ticket for the owner's own machines.
    account: Option<String>,
    ttl_secs: Option<u64>,
    uses: Option<u32>,
    #[serde(default)]
    allow_docker: bool,
}

async fn invite(axum::extract::State(st): S, a: Api, Json(b): Json<InviteBody>) -> Response {
    let invite = b.account.map(|account| Invite {
        account,
        allow_docker: b.allow_docker,
    });
    let ttl_secs = b
        .ttl_secs
        .unwrap_or(if invite.is_some() { 7 * 86_400 } else { 86_400 });
    simple(
        st,
        a,
        AdminOp::CreateJoinToken {
            ttl_secs,
            uses: b.uses,
            invite,
        },
    )
    .await
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default)]
    after: u64,
    limit: Option<u32>,
}

/// Audit log entries (admins only), oldest first.
async fn audit(axum::extract::State(st): S, a: Api, Query(q): Query<AuditQuery>) -> Response {
    simple(
        st,
        a,
        AdminOp::Audit {
            after: q.after,
            limit: q.limit.unwrap_or(100).min(10_000),
        },
    )
    .await
}

/// The audit chain checked, with its signed head.
async fn audit_verify(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::AuditVerify).await
}

/// Any admin call, as the JSON form of `AdminOp` (e.g.
/// `{"Drain": {"node": "mac-mini"}}` or `"Status"`).
async fn op(axum::extract::State(st): S, a: Api, Json(op): Json<AdminOp>) -> Response {
    simple(st, a, op).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every API route is documented, and nothing else is.
    #[test]
    fn openapi_lists_the_routes() {
        let doc: serde_json::Value = serde_json::from_str(OPENAPI).unwrap();
        assert_eq!(doc["openapi"], "3.1.0");
        let mut documented: Vec<String> =
            doc["paths"].as_object().unwrap().keys().cloned().collect();
        documented.sort();
        let src = include_str!("api.rs");
        let mut routed: Vec<String> = src
            .lines()
            .filter_map(|l| l.trim().strip_prefix(".route(\""))
            .map(|l| format!("/api/v1{}", &l[..l.find('"').unwrap()]))
            .collect();
        routed.push("/api/openapi.json".into());
        routed.sort();
        assert_eq!(documented, routed);
    }

    #[test]
    fn payload_drops_the_tag() {
        let r = payload(AdminResponse::Drained { moved: 2 });
        assert_eq!(r.status(), StatusCode::OK);
    }
}
