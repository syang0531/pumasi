//! `/scim/v2` (1.12): SCIM requests from an identity provider, passed to
//! the control plane as they came ([`pumasi_proto::scim`]). The dashboard
//! only counts wrong tokens against the caller's address, like failed
//! sign-ins.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use pumasi_proto::{
    msg::{AdminOp, AdminResponse},
    scim::{SCIM_PREFIX, ScimRequest},
    sso::Hidden,
};

use crate::{St, limit::ClientIp};

const CONTENT_TYPE: &str = "application/scim+json; charset=utf-8";

fn scim_error(status: StatusCode, detail: &str) -> Response {
    let body = serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "status": status.as_u16().to_string(),
        "detail": detail,
    });
    (
        status,
        [(header::CONTENT_TYPE, CONTENT_TYPE)],
        body.to_string(),
    )
        .into_response()
}

pub async fn handle(
    State(st): State<St>,
    ClientIp(ip): ClientIp,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(r) = st.limiter.check(ip) {
        return r;
    }
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(|t| t.trim().to_string())
        .unwrap_or_default();
    let Ok(body) = String::from_utf8(body.to_vec()) else {
        return scim_error(StatusCode::BAD_REQUEST, "the body is not UTF-8");
    };
    let req = ScimRequest {
        token: Hidden(token),
        method: method.to_string(),
        path: uri
            .path()
            .strip_prefix(SCIM_PREFIX)
            .unwrap_or("")
            .to_string(),
        query: uri.query().unwrap_or("").to_string(),
        body,
        base_url: format!("{}{SCIM_PREFIX}", st.public_url),
    };
    match st
        .backend
        .call(st.backend.own_secret(), AdminOp::Scim(Box::new(req)))
        .await
    {
        Ok(AdminResponse::Scim(r)) => {
            if r.status == 401 {
                st.limiter.failed(ip);
            }
            let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_GATEWAY);
            if r.body.is_empty() {
                return status.into_response();
            }
            let mut resp = (status, r.body).into_response();
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE));
            resp
        }
        Ok(AdminResponse::Error(e)) if e.contains("too many") => {
            st.limiter.failed(ip);
            scim_error(StatusCode::TOO_MANY_REQUESTS, &e)
        }
        Ok(AdminResponse::Error(e)) => scim_error(StatusCode::BAD_GATEWAY, &e),
        Ok(_) => scim_error(
            StatusCode::NOT_IMPLEMENTED,
            "the control plane does not know SCIM (it is older than 1.12)",
        ),
        Err(e) => scim_error(StatusCode::SERVICE_UNAVAILABLE, &format!("{e:#}")),
    }
}
