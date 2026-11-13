//! The CU price list and settlement policy; admins change them here
//! (1.7) or with `pumasi prices set`.

use anyhow::Context as _;
use axum::{Form, http::HeaderMap, response::Response};
use maud::{Markup, html};
use pumasi_proto::{
    economy::{
        LedgerMode, Policy, format_cu, format_rate, parse_cu, parse_rate, parse_rate_permille,
    },
    job::parse_duration_ms,
    msg::{AdminOp, AdminResponse},
    store::{StoragePolicy, parse_size},
};
use serde::Deserialize;

use super::{Ctx, Nav, hx};
use crate::{fmt, i18n::Lang};

pub async fn page(ctx: Ctx) -> Response {
    let l = ctx.l;
    let policy = match ctx.call(AdminOp::GetPolicy).await {
        Ok(AdminResponse::Policy(p)) => p,
        Ok(_) => return ctx.fail(anyhow::anyhow!("unexpected response"), false),
        Err(e) => return ctx.fail(e, false),
    };
    let storage = match ctx.call(AdminOp::GetStoragePolicy).await {
        Ok(AdminResponse::StoragePolicy { policy, class }) => Some((policy, class)),
        _ => None,
    };
    let r = &policy.rates;
    let cu = |u: i64| format!("{} CU", format_rate(u));
    let body = html! {
        div class="page-head" { h1 { (l.t("Prices", "요금")) } }
        p { (l.t(
            "1 CU is one reference vCPU for one hour. Runs are billed for what they reserve, scaled by the node's measured CPU score.",
            "1 CU는 기준 vCPU 하나를 한 시간 쓰는 양입니다. 실행은 예약한 자원만큼, 노드의 측정된 CPU 점수에 비례해 과금됩니다."
        )) }
        div class="cols" {
            section {
                h2 { (l.t("Rates", "단가")) }
                dl class="facts" {
                    dt { (l.t("vCPU hour (reference)", "vCPU 시간(기준)")) } dd { "1 CU" }
                    dt { (l.t("Memory, per GiB-hour", "메모리, GiB-시간당")) } dd { (cu(r.memory_gib_hour)) }
                    dt { (l.t("GPU, per TFLOPS-hour", "GPU, TFLOPS-시간당")) } dd { (cu(r.gpu_tflops_hour)) }
                    dt { (l.t("GPU memory, per GiB-hour", "GPU 메모리, GiB-시간당")) } dd { (cu(r.gpu_vram_gib_hour)) }
                    dt { (l.t("Storage, per GiB-month (SSD)", "저장, GiB-월당(SSD)")) } dd { (cu(r.storage_gib_month)) }
                    @if let Some((s, class)) = &storage {
                        dt { (l.t("Storage on HDD / NVMe", "HDD / NVMe 저장")) } dd { (cu(s.hdd_gib_month)) " / " (cu(s.nvme_gib_month)) }
                        dt { (l.t("Control plane disk", "컨트롤 플레인 디스크")) } dd { @match class {
                            pumasi_proto::perf::DiskClass::Unknown => (l.t("not given (billed as SSD; `pumasi cluster start --storage-class` sets it)",
                                                                          "지정 안 됨 (SSD 요금; `pumasi cluster start --storage-class`로 지정)")),
                            c => (format!("{c:?}").to_lowercase()),
                        } }
                        dt { (l.t("Default storage quota", "기본 저장 쿼터")) } dd {
                            @if let Some(q) = s.default_quota { (fmt::format_size(q)) } @else { (l.t("none", "없음")) }
                        }
                    }
                    dt { (l.t("Egress, per GiB", "외부 전송, GiB당")) } dd { (cu(r.egress_gib)) }
                }
            }
            section {
                h2 { (l.t("Settlement", "정산")) }
                dl class="facts" {
                    dt { (l.t("Ledger", "원장")) } dd { @match policy.ledger {
                        LedgerMode::Prepaid => { (l.t("prepaid: the owner grants CU", "선불: 소유자가 CU를 지급")) }
                        LedgerMode::Mutual { credit_limit } => {
                            (l.t("mutual (품앗이): lending earns, using spends, down to", "상호(품앗이): 빌려주면 벌고 쓰면 차감, 최저")) " -" (fmt::cu(credit_limit))
                        }
                    } }
                    dt { (l.t("Earnings held for", "수입 보류 기간")) } dd { (fmt::human_duration(policy.hold_secs)) }
                    dt { (l.t("Results checked", "결과 검증 비율")) } dd { (format!("{:.1}%", policy.verify_permille as f64 / 10.0)) }
                    dt { (l.t("Failed checks before suspension", "정지까지 허용 검증 실패")) } dd { (policy.max_strikes) }
                }
                p class="dim" { (l.t("Admins change these below or with `pumasi prices set`.", "관리자는 아래나 `pumasi prices set`으로 바꿉니다.")) }
            }
        }
    };
    let bottom = if ctx.me.is_admin() {
        form(l, &policy, storage.as_ref().map(|(s, _)| s))
    } else {
        html! {}
    };
    ctx.page(l.t("Prices", "요금"), Nav::Prices, body, None, bottom)
}

fn form(l: Lang, p: &Policy, storage: Option<&StoragePolicy>) -> Markup {
    let r = &p.rates;
    let (mutual, limit) = match p.ledger {
        LedgerMode::Prepaid => (false, None),
        LedgerMode::Mutual { credit_limit } => (true, Some(credit_limit)),
    };
    html! {
        section {
            h2 { (l.t("Change prices", "요금 바꾸기")) }
            form #prices-form class="stack settings" hx-post="/prices" hx-target="#flash"
                hx-confirm=(l.t("Save the prices? New runs are billed at them.", "요금을 저장할까요? 새 실행부터 이 요금으로 과금됩니다.")) {
                p class="dim" { (l.t("Rates in CU; changes apply to runs that start after saving.", "단가는 CU 단위이고, 저장한 뒤 시작하는 실행부터 적용됩니다.")) }
                div class="grid2" {
                    label { (l.t("Memory, per GiB-hour", "메모리, GiB-시간당")) input name="memory_gib_hour" value=(format_rate(r.memory_gib_hour)); }
                    label { (l.t("GPU, per TFLOPS-hour", "GPU, TFLOPS-시간당")) input name="gpu_tflops_hour" value=(format_rate(r.gpu_tflops_hour)); }
                    label { (l.t("GPU memory, per GiB-hour", "GPU 메모리, GiB-시간당")) input name="gpu_vram_gib_hour" value=(format_rate(r.gpu_vram_gib_hour)); }
                    label { (l.t("Storage, per GiB-month (SSD)", "저장, GiB-월당(SSD)")) input name="storage_gib_month" value=(format_rate(r.storage_gib_month)); }
                    label { (l.t("Egress, per GiB", "외부 전송, GiB당")) input name="egress_gib" value=(format_rate(r.egress_gib)); }
                    @if let Some(s) = storage {
                        label { (l.t("Storage on HDD, per GiB-month", "HDD 저장, GiB-월당")) input name="hdd_gib_month" value=(format_rate(s.hdd_gib_month)); }
                        label { (l.t("Storage on NVMe, per GiB-month", "NVMe 저장, GiB-월당")) input name="nvme_gib_month" value=(format_rate(s.nvme_gib_month)); }
                        label { (l.t("Default storage quota (none: no limit)", "기본 저장 쿼터 (none: 무제한)")) input name="default_quota"
                            value=(s.default_quota.map(fmt::format_size).unwrap_or_else(|| "none".into())); }
                    }
                    label { (l.t("Ledger", "원장")) select name="ledger" {
                        option value="prepaid" selected[!mutual] { (l.t("prepaid", "선불")) }
                        option value="mutual" selected[mutual] { (l.t("mutual (품앗이)", "상호(품앗이)")) }
                    } }
                    label { (l.t("Credit limit (mutual), CU", "신용 한도(상호), CU")) input name="credit_limit" value=(format_cu(limit.unwrap_or(20_000))); }
                    label { (l.t("Earnings held for", "수입 보류 기간")) input name="hold" value=(fmt::human_duration(p.hold_secs)); }
                    label { (l.t("Results checked", "결과 검증 비율")) input name="verify_rate" value=(format!("{:.1}%", p.verify_permille as f64 / 10.0)); }
                    label { (l.t("Failed checks before suspension", "정지까지 허용 검증 실패")) input name="max_strikes" type="number" min="1" value=(p.max_strikes); }
                }
                div class="row" { button type="submit" { (l.t("Save prices", "요금 저장")) } }
            }
        }
    }
}

#[derive(Deserialize, Default)]
pub struct PricesForm {
    memory_gib_hour: String,
    gpu_tflops_hour: String,
    gpu_vram_gib_hour: String,
    storage_gib_month: String,
    egress_gib: String,
    hdd_gib_month: Option<String>,
    nvme_gib_month: Option<String>,
    default_quota: Option<String>,
    ledger: String,
    credit_limit: Option<String>,
    hold: String,
    verify_rate: String,
    max_strikes: String,
}

/// Applies the form to the policies.
pub fn apply_form(f: &PricesForm, p: &mut Policy, s: &mut StoragePolicy) -> anyhow::Result<()> {
    let rate = |v: &str, what: &str| parse_rate(v.trim()).with_context(|| what.to_string());
    p.rates.memory_gib_hour = rate(&f.memory_gib_hour, "memory")?;
    p.rates.gpu_tflops_hour = rate(&f.gpu_tflops_hour, "GPU")?;
    p.rates.gpu_vram_gib_hour = rate(&f.gpu_vram_gib_hour, "GPU memory")?;
    p.rates.storage_gib_month = rate(&f.storage_gib_month, "storage")?;
    p.rates.egress_gib = rate(&f.egress_gib, "egress")?;
    if let Some(v) = &f.hdd_gib_month {
        s.hdd_gib_month = rate(v, "HDD storage")?;
    }
    if let Some(v) = &f.nvme_gib_month {
        s.nvme_gib_month = rate(v, "NVMe storage")?;
    }
    if let Some(v) = &f.default_quota {
        s.default_quota = match v.trim() {
            "" | "none" | "off" => None,
            q => Some(parse_size(q).context("default quota")?),
        };
    }
    p.ledger = match f.ledger.as_str() {
        "prepaid" => LedgerMode::Prepaid,
        "mutual" => LedgerMode::Mutual {
            credit_limit: parse_cu(f.credit_limit.as_deref().unwrap_or("20").trim())
                .context("credit limit")?,
        },
        other => anyhow::bail!("ledger is prepaid or mutual, not {other:?}"),
    };
    p.hold_secs = parse_duration_ms(f.hold.trim()).context("hold")? / 1000;
    p.verify_permille = parse_rate_permille(&f.verify_rate).context("results checked")?;
    p.max_strikes = f
        .max_strikes
        .trim()
        .parse()
        .context("failed checks: a whole number")?;
    Ok(())
}

pub async fn save(ctx: Ctx, headers: HeaderMap, Form(f): Form<PricesForm>) -> Response {
    let res = async {
        let AdminResponse::Policy(mut p) = ctx.call(AdminOp::GetPolicy).await? else {
            anyhow::bail!("unexpected response");
        };
        let old_storage = match ctx.call(AdminOp::GetStoragePolicy).await {
            Ok(AdminResponse::StoragePolicy { policy, .. }) => Some(policy),
            _ => None,
        };
        let mut s = old_storage.unwrap_or_default();
        apply_form(&f, &mut p, &mut s)?;
        ctx.call(AdminOp::SetPolicy(p)).await?;
        if old_storage.as_ref().is_some_and(|o| *o != s) {
            ctx.call(AdminOp::SetStoragePolicy(s)).await?;
        }
        anyhow::Ok(())
    }
    .await;
    match res {
        Ok(()) => {
            let msg = ctx.t("Prices saved.", "요금을 저장했습니다.").to_string();
            let mut r = ctx.done(msg);
            r.headers_mut()
                .insert("hx-refresh", axum::http::HeaderValue::from_static("true"));
            r
        }
        Err(e) => ctx.fail(e, hx(&headers)),
    }
}
