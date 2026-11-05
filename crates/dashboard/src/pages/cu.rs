//! Compute units: balances, the ledger, and (admins) grants and payouts.
//! Receipts are on each job's page.

use axum::{
    Form,
    extract::{Path, Query},
    http::HeaderMap,
    response::Response,
};
use maud::html;
use pumasi_proto::{
    economy::parse_cu,
    msg::{AccountView, AdminOp, AdminResponse, LedgerEntry},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx, table, tile};
use crate::fmt;

#[derive(Deserialize)]
pub struct CuQuery {
    account: Option<String>,
}

pub async fn page(ctx: Ctx, Query(q): Query<CuQuery>) -> Response {
    match render(&ctx, q.account.filter(|a| !a.is_empty())).await {
        Ok(r) => r,
        Err(e) => ctx.fail(e, false),
    }
}

async fn render(ctx: &Ctx, account: Option<String>) -> anyhow::Result<Response> {
    let l = ctx.l;
    let accounts: Vec<AccountView> = match ctx.call(AdminOp::Accounts).await? {
        AdminResponse::Accounts(a) => a,
        _ => anyhow::bail!("unexpected response"),
    };
    let ledger: Vec<LedgerEntry> = match ctx
        .call(AdminOp::Ledger {
            account: account.clone(),
            limit: 100,
        })
        .await?
    {
        AdminResponse::Ledger(e) => e,
        _ => anyhow::bail!("unexpected response"),
    };
    let total: i64 = accounts.iter().map(|a| a.balance).sum();
    let held: i64 = accounts.iter().map(|a| a.held).sum();
    let rows = accounts
        .iter()
        .map(|a| {
            html! {
                tr data-account=(a.name) {
                    td { a href={"/cu?account=" (super::encode(&a.name))} { strong { (a.name) } } }
                    td class=[(a.balance < 0).then_some("neg")] { (fmt::cu(a.balance)) }
                    td { (fmt::cu(a.held)) }
                    td { (fmt::cu(a.spent)) }
                    td { (fmt::cu(a.earned)) }
                }
            }
        })
        .collect();
    let entries = ledger
        .iter()
        .map(|e| {
            html! {
                tr {
                    td { (e.seq) }
                    td { (fmt::time(e.unix)) }
                    td { (e.account) }
                    td { (e.kind.to_string()) }
                    td class=[(e.amount < 0).then_some("neg")] { (fmt::cu(e.amount)) }
                    td { @if let Some(t) = &e.task { a href={"/jobs/" (t.job) "/tasks/" (t.index)} { code { (t.job) } " #" (t.index) } } }
                    td { (e.memo) }
                }
            }
        })
        .collect();
    let top = html! {
        div class="page-head" { h1 { (l.t("Compute units (CU)", "컴퓨팅 유닛(CU)")) } }
    };
    let live = html! {
        section class="tiles" {
            (tile(l.t("Spendable, all accounts", "사용 가능(전체 계정)"), fmt::cu(total), None))
            (tile(l.t("Held earnings", "보류된 수입"), fmt::cu(held), None))
            (tile(l.t("Accounts", "계정"), accounts.len().to_string(), None))
        }
        section {
            h2 { (l.t("Accounts", "계정")) }
            (table(&[l.t("Account", "계정"), l.t("Balance", "잔액"), l.t("Held", "보류"), l.t("Spent", "사용"), l.t("Earned", "수입")],
                rows, l.t("No accounts yet.", "아직 계정이 없습니다.")))
        }
        section {
            h2 {
                (l.t("Ledger", "원장"))
                @if let Some(a) = &account { " · " (a) " " a class="dim" href="/cu" { (l.t("(all)", "(전체)")) } }
            }
            (table(&["#", l.t("Time", "시각"), l.t("Account", "계정"), l.t("Kind", "종류"), l.t("Amount", "금액"), l.t("Run", "실행"), l.t("Memo", "메모")],
                entries, l.t("No entries.", "기록이 없습니다.")))
        }
    };
    let bottom = html! {
        @if ctx.me.is_admin() {
            section {
                h2 { (l.t("Grant or pay out CU", "CU 지급·인출")) }
                p class="dim" { (l.t("Bookkeeping only; no money moves. Prepaid mode only.", "장부 기록일 뿐 돈은 오가지 않습니다. 선불 모드에서만 됩니다.")) }
                form class="inline" hx-post="/cu/deposit" hx-target="#flash" {
                    input name="account" placeholder=(l.t("account", "계정")) required size="12";
                    input name="amount" placeholder="10.5" required size="8" inputmode="decimal";
                    input name="memo" placeholder=(l.t("memo", "메모")) size="16";
                    button type="submit" { (l.t("Grant", "지급")) }
                    button type="submit" class="secondary" formaction="/cu/payout" hx-post="/cu/payout" { (l.t("Pay out", "인출")) }
                }
            }
        }
    };
    Ok(ctx.page("CU", Nav::Cu, top, Some(("10s", live)), bottom))
}

#[derive(Deserialize)]
pub struct MoveForm {
    account: String,
    amount: String,
    #[serde(default)]
    memo: String,
}

pub async fn action(
    ctx: Ctx,
    headers: HeaderMap,
    Path(action): Path<String>,
    Form(f): Form<MoveForm>,
) -> Response {
    let res = async {
        let amount = parse_cu(&f.amount)?;
        let account = f.account.trim().to_string();
        let op = match action.as_str() {
            "deposit" => AdminOp::Deposit {
                account: account.clone(),
                amount,
                memo: f.memo,
            },
            "payout" => AdminOp::Payout {
                account: account.clone(),
                amount,
                memo: f.memo,
            },
            _ => anyhow::bail!("no such action"),
        };
        ctx.call(op).await?;
        Ok(format!(
            "{account}: {}{}",
            if action == "deposit" { "+" } else { "-" },
            fmt::cu(amount)
        ))
    }
    .await;
    match res {
        Ok(m) => ctx.done(m),
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
