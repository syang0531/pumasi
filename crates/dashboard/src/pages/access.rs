//! Admins only: join tickets for the owner's machines, invites for other
//! people's machines, and access tickets (roles) for people.

use axum::{
    Form,
    extract::Path,
    http::HeaderMap,
    response::{Html, IntoResponse, Response},
};
use maud::{Markup, html};
use pumasi_proto::{
    job::parse_duration_ms,
    manage::{AccessRole, AccessView},
    msg::{AdminOp, AdminResponse, Invite, JoinTokenView},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, role_text, table};
use crate::{fmt, i18n::Lang};

const INSTALL_SH: &str = "https://github.com/syang0531/pumasi/releases/latest/download/install.sh";
const INSTALL_PS1: &str =
    "https://github.com/syang0531/pumasi/releases/latest/download/install.ps1";

pub async fn page(ctx: Ctx) -> Response {
    let l = ctx.l;
    let list: Vec<AccessView> = match ctx.call(AdminOp::ListAccess).await {
        Ok(AdminResponse::AccessList(a)) => a,
        Ok(_) => Vec::new(),
        Err(e) => return ctx.fail(e, false),
    };
    let tokens: Vec<JoinTokenView> = match ctx.call(AdminOp::ListJoinTokens).await {
        Ok(AdminResponse::JoinTokens(t)) => t,
        Ok(_) => Vec::new(),
        // A control plane older than 1.2 has no list: show none.
        Err(_) => Vec::new(),
    };
    let token_rows = tokens
        .iter()
        .map(|t| {
            html! {
                tr {
                    td { code { (t.id) } }
                    td { @match &t.invite {
                        Some(i) => { (l.t("Invite: ", "초대: ")) (i.account) @if i.allow_docker { " (Docker)" } }
                        None => { (l.t("Your machines", "내 기기")) }
                    } }
                    td { @match t.uses_left { Some(n) => { (n) }, None => { (l.t("any", "제한 없음")) } } }
                    td { (fmt::time(t.expires_unix)) }
                    td { button class="link danger" hx-post="/access/revoke-token" hx-vals=(serde_json::json!({"id": t.id}).to_string())
                        hx-target="#flash" hx-confirm=(l.t("Revoke this join ticket? Machines that already joined stay.", "이 참가 티켓을 취소할까요? 이미 참가한 기기는 그대로 남습니다."))
                        { (l.t("Revoke", "취소")) } }
                }
            }
        })
        .collect();
    let rows = list
        .iter()
        .map(|a| {
            html! {
                tr {
                    td { code { (a.id) } }
                    td { (a.name) }
                    td { (role_text(l, &a.role)) }
                    td { (fmt::time(a.created_unix)) }
                    td { @if let Some(e) = a.expires_unix { (fmt::time(e)) } @else { (l.t("never", "없음")) } }
                    td { button class="link danger" hx-post="/access/revoke" hx-vals=(serde_json::json!({"id": a.id}).to_string())
                        hx-target="#flash" hx-confirm=(l.t("Revoke this ticket? Its holder is signed out at the next call.", "이 티켓을 폐기할까요? 다음 호출부터 거부됩니다."))
                        { (l.t("Revoke", "폐기")) } }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Invites and access", "초대와 접근 권한")) } }
        div class="cols" {
            section {
                h2 { (l.t("Add one of your machines", "내 기기 추가")) }
                p class="dim" { (l.t("A join ticket for machines you own: they run every executor they offer.",
                                     "직접 소유한 기기용 참여 티켓입니다. 기기가 제공하는 모든 실행기를 씁니다.")) }
                form class="inline" hx-post="/access/token" hx-target="#token-result" {
                    label { (l.t("Valid for", "유효 기간")) " " input name="ttl" value="24h" size="5"; }
                    label { (l.t("Machines", "기기 수")) " " input name="uses" type="number" min="1" placeholder=(l.t("any", "제한 없음")) size="5"; }
                    button type="submit" { (l.t("Create ticket", "티켓 만들기")) }
                }
                div #token-result {}
            }
            section {
                h2 { (l.t("Invite someone's machine", "다른 사람의 기기 초대")) }
                p class="dim" { (l.t("It joins as a provider: WASM and, if it opts in, microVM jobs; earns CU for the account; a sample of its results is checked.",
                                     "제공자로 참여합니다. WASM과(동의하면) microVM 작업을 실행하고, 계정에 CU를 벌며, 결과 일부를 검증합니다.")) }
                form class="inline" hx-post="/access/invite" hx-target="#invite-result" {
                    input name="account" placeholder=(l.t("account", "계정")) required size="12" pattern="[a-z0-9_-]+";
                    label { (l.t("Valid for", "유효 기간")) " " input name="ttl" value="7d" size="5"; }
                    label { (l.t("Machines", "기기 수")) " " input name="uses" type="number" min="1" value="1" size="4"; }
                    label { input type="checkbox" name="allow_docker" value="true"; " " (l.t("Docker too", "Docker 허용")) }
                    button type="submit" { (l.t("Create invite", "초대 만들기")) }
                }
                div #invite-result {}
            }
        }
    };
    let bottom = html! {
        section {
            h2 { (l.t("New access ticket", "새 액세스 티켓")) }
            p class="dim" { (l.t("For people: sign in to this dashboard or `pumasi login` with one. Admin: everything. Submitter: one account's jobs and datasets. Viewer: read only.",
                                 "사람에게 주는 티켓입니다. 이 대시보드나 `pumasi login`에 씁니다. 관리자: 전부. 제출자: 한 계정의 작업과 데이터셋. 열람자: 읽기만.")) }
            form class="inline" hx-post="/access/create" hx-target="#access-result" {
                input name="name" placeholder=(l.t("name", "이름")) required size="12";
                select name="role" {
                    option value="viewer" { (l.t("viewer", "열람자")) }
                    option value="submitter" { (l.t("submitter", "제출자")) }
                    option value="admin" { (l.t("admin", "관리자")) }
                }
                input name="account" placeholder=(l.t("account (submitter)", "계정(제출자)")) size="14";
                label { (l.t("Valid for", "유효 기간")) " " input name="ttl" placeholder=(l.t("forever", "무기한")) size="6"; }
                button type="submit" { (l.t("Create", "만들기")) }
            }
            div #access-result {}
        }
    };
    let live = html! {
        section {
            h2 { (l.t("Usable join tickets and invites", "쓸 수 있는 참가 티켓과 초대")) }
            (table(
                &[
                    "ID",
                    l.t("For", "대상"),
                    l.t("Machines left", "남은 기기 수"),
                    l.t("Expires", "만료"),
                    "",
                ],
                token_rows,
                l.t("No usable join tickets.", "쓸 수 있는 참가 티켓이 없습니다."),
            ))
        }
        section {
            h2 { (l.t("Access tickets", "액세스 티켓")) }
            (table(
                &[
                    "ID",
                    l.t("Name", "이름"),
                    l.t("Role", "역할"),
                    l.t("Created", "생성"),
                    l.t("Expires", "만료"),
                    "",
                ],
                rows,
                l.t("No access tickets.", "액세스 티켓이 없습니다."),
            ))
        }
    };
    ctx.page(
        l.t("Invites", "초대"),
        Nav::Access,
        html! { (top) (bottom) },
        Some(("30s", live)),
        html! {},
    )
}

fn ticket_box(l: Lang, ticket: &str, note: Markup, join: bool) -> Markup {
    html! {
        div class="ticket" {
            (note)
            textarea readonly rows="3" class="copy" data-ticket { (ticket) }
            button type="button" class="secondary" data-copy { (l.t("Copy", "복사")) }
            @if join {
                details open {
                    summary { (l.t("Install the agent with it", "이 티켓으로 에이전트 설치")) }
                    p { "Linux, macOS:" }
                    pre { "curl -fsSL " (INSTALL_SH) " | sudo sh -s -- --ticket " (ticket) }
                    p { "Windows (PowerShell, " (l.t("administrator", "관리자")) "):" }
                    pre { "& ([scriptblock]::Create((irm " (INSTALL_PS1) "))) -Ticket " (ticket) }
                    p { (l.t("Or, with pumasi installed:", "또는 pumasi가 설치되어 있다면:")) }
                    pre { "pumasi agent join " (ticket) }
                }
            }
        }
    }
}

#[derive(Deserialize, Default)]
pub struct AccessForm {
    ttl: Option<String>,
    uses: Option<String>,
    account: Option<String>,
    allow_docker: Option<String>,
    name: Option<String>,
    role: Option<String>,
    id: Option<String>,
}

fn opt(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

fn ttl_secs(s: Option<&str>, default: &str) -> anyhow::Result<u64> {
    Ok(parse_duration_ms(s.unwrap_or(default))? / 1000)
}

pub async fn action(
    ctx: Ctx,
    headers: HeaderMap,
    Path(action): Path<String>,
    Form(f): Form<AccessForm>,
) -> Response {
    let l = ctx.l;
    let res: anyhow::Result<Response> = async {
        let uses = opt(&f.uses).map(str::parse::<u32>).transpose()?;
        match action.as_str() {
            "token" | "invite" => {
                let invite = if action == "invite" {
                    let account = opt(&f.account).ok_or_else(|| anyhow::anyhow!("an account is needed"))?;
                    Some(Invite {
                        account: account.to_string(),
                        allow_docker: f.allow_docker.is_some(),
                    })
                } else {
                    None
                };
                let ttl = ttl_secs(opt(&f.ttl), if invite.is_some() { "7d" } else { "24h" })?;
                match ctx
                    .call(AdminOp::CreateJoinToken {
                        ttl_secs: ttl,
                        uses,
                        invite,
                    })
                    .await?
                {
                    AdminResponse::JoinToken {
                        ticket,
                        expires_unix,
                    } => Ok((
                        [("hx-trigger", "refresh")],
                        Html(
                            ticket_box(
                                l,
                                &ticket,
                                html! { p { (l.t("Valid until", "유효 기한")) " " (fmt::time(expires_unix)) } },
                                true,
                            )
                            .into_string(),
                        ),
                    )
                        .into_response()),
                    _ => anyhow::bail!("unexpected response"),
                }
            }
            "create" => {
                let name = opt(&f.name).ok_or_else(|| anyhow::anyhow!("a name is needed"))?;
                let role = match opt(&f.role).unwrap_or("viewer") {
                    "admin" => AccessRole::Admin,
                    "submitter" => AccessRole::Submitter {
                        account: opt(&f.account)
                            .ok_or_else(|| anyhow::anyhow!("a submitter needs an account"))?
                            .to_string(),
                    },
                    _ => AccessRole::Viewer,
                };
                role.validate()?;
                let ttl_secs = opt(&f.ttl).map(|t| ttl_secs(Some(t), "")).transpose()?;
                match ctx
                    .call(AdminOp::CreateAccess {
                        name: name.to_string(),
                        role,
                        ttl_secs,
                    })
                    .await?
                {
                    AdminResponse::AccessTicket { ticket, view } => Ok((
                        [("hx-trigger", "refresh")],
                        Html(
                            ticket_box(
                                l,
                                &ticket,
                                html! { p { strong { (view.name) } " · " (role_text(l, &view.role)) " — "
                                    (l.t("shown only now; hand it over privately.", "지금만 보입니다. 안전하게 전달하세요.")) } },
                                false,
                            )
                            .into_string(),
                        ),
                    )
                        .into_response()),
                    _ => anyhow::bail!("unexpected response"),
                }
            }
            "revoke-token" => {
                let id = opt(&f.id).ok_or_else(|| anyhow::anyhow!("which ticket?"))?;
                ctx.call(AdminOp::RevokeJoinToken { id: id.to_string() }).await?;
                Ok(ctx.done(l.t("Revoked.", "취소했습니다.")))
            }
            "revoke" => {
                let id = opt(&f.id).ok_or_else(|| anyhow::anyhow!("which ticket?"))?;
                ctx.call(AdminOp::RevokeAccess { id: id.to_string() }).await?;
                Ok(ctx.done(l.t("Revoked.", "폐기했습니다.")))
            }
            _ => anyhow::bail!("no such action"),
        }
    }
    .await;
    match res {
        Ok(r) => r,
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
