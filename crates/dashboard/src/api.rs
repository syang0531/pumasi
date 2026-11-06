//! The JSON API (`/api/v1`), for scripts and other tools. It answers with
//! the same views the CLI gets (job ids as hex strings), takes a signed-in
//! browser session or `Authorization: Bearer <ticket>`, and the control
//! plane applies the ticket's role. `POST /api/v1/op` takes any admin call
//! as JSON. Described in `assets/openapi.json`.

// Handlers return early with a finished response as the error.
#![allow(clippy::result_large_err)]

use axum::{
    Json, Router,
    extract::{Multipart, Path, Query},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use pumasi_proto::{
    job::JobId,
    manage::NodeEdit,
    msg::{AdminOp, AdminResponse, Invite},
    receipt::ReceiptRow,
};
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::{St, auth::Api, pages};

pub const OPENAPI: &str = include_str!("../assets/openapi.json");

pub fn routes() -> Router<St> {
    Router::new()
        .route("/whoami", get(whoami))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/events", get(events))
        .route("/alerts", get(alerts))
        .route("/jobs", post(submit))
        .route("/jobs/{id}", get(job))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/receipts", get(receipts))
        .route("/jobs/{id}/outputs.zip", get(outputs_zip))
        .route("/jobs/{id}/tasks/{index}/logs", get(logs))
        .route("/nodes/{node}/{action}", post(node_action))
        .route("/accounts", get(accounts))
        .route("/ledger", get(ledger))
        .route("/datasets", get(datasets))
        .route("/storage", get(storage))
        .route("/prices", get(prices))
        .route("/invites", post(invite))
        .route("/op", post(op))
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

async fn whoami(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::WhoAmI).await
}

async fn status(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Status).await
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

async fn alerts(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Alerts).await
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
            return payload(r);
        }
        tokio::time::sleep(pause.min(deadline - now)).await;
        pause = (pause * 2).min(Duration::from_secs(2));
    }
}

async fn submit(axum::extract::State(st): S, a: Api, form: Multipart) -> Response {
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
        _ => {
            return error(
                StatusCode::NOT_FOUND,
                "actions: drain, cordon, uncordon, benchmark",
            );
        }
    };
    simple(st, a, op).await
}

async fn accounts(axum::extract::State(st): S, a: Api) -> Response {
    simple(st, a, AdminOp::Accounts).await
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
