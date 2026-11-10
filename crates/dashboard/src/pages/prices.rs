//! The CU price list and settlement policy (read only; `pumasi prices
//! set` changes it).

use axum::response::Response;
use maud::html;
use pumasi_proto::{
    economy::{LedgerMode, format_rate},
    msg::{AdminOp, AdminResponse},
};

use super::{Ctx, Nav};
use crate::fmt;

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
                p class="dim" { (l.t("Admins change these with `pumasi prices set`.", "관리자는 `pumasi prices set`으로 바꿉니다.")) }
            }
        }
    };
    ctx.page(l.t("Prices", "요금"), Nav::Prices, body, None, html! {})
}
