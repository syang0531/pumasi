//! Messages by key (1.15): events and alerts carry a key and arguments
//! next to their English text, so the Korean dashboard can show them in
//! Korean. The English text the control plane stores comes from the same
//! table ([`Text::en`]), so the two never drift apart.
//!
//! Arguments are what changes from one message to the next: node and job
//! names, counts, sizes, durations. Reasons that come from elsewhere (an
//! agent's error, a rollout's failure) stay in English inside them.

use serde::{Deserialize, Serialize};

/// A message by key, with its arguments and, for messages that wrap
/// another (`resolved: ...`), the message inside.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Text {
    pub key: String,
    pub args: Vec<String>,
    pub inner: Option<Box<Text>>,
}

impl Text {
    pub fn new<I, S>(key: &str, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        Self {
            key: key.to_string(),
            args: args.into_iter().map(|a| a.to_string()).collect(),
            inner: None,
        }
    }

    /// `key` around `inner`: `resolved: {inner}`.
    pub fn wrap<I, S>(key: &str, args: I, inner: Text) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        Self {
            inner: Some(Box::new(inner)),
            ..Self::new(key, args)
        }
    }

    /// The message in Korean (`ko`) or English; `None` for a key this build
    /// does not know (a newer control plane's), or arguments that do not
    /// fit it.
    pub fn render(&self, ko: bool) -> Option<String> {
        let (_, en, kr) = TEXTS.iter().find(|(k, _, _)| *k == self.key)?;
        let template = if ko { kr } else { en };
        let inner = match &self.inner {
            Some(t) => Some(t.render(ko)?),
            None => None,
        };
        let mut out = String::with_capacity(template.len() + 32);
        let mut rest = *template;
        while let Some(open) = rest.find('{') {
            out.push_str(&rest[..open]);
            let close = rest[open..].find('}')? + open;
            let name = &rest[open + 1..close];
            if name == "inner" {
                out.push_str(inner.as_deref()?);
            } else {
                let i: usize = name.parse().ok()?;
                out.push_str(self.args.get(i)?);
            }
            rest = &rest[close + 1..];
        }
        out.push_str(rest);
        Some(out)
    }

    /// The English message (the key itself when unknown).
    pub fn en(&self) -> String {
        self.render(false).unwrap_or_else(|| self.key.clone())
    }
}

/// Every message: key, English, Korean. `{0}`, `{1}`... are the arguments
/// and `{inner}` the wrapped message. Korean avoids particles after an
/// argument (이/가, 을/를), which depend on the word.
pub const TEXTS: &[(&str, &str, &str)] = &[
    // Nodes.
    (
        "node.joined",
        "node {0} joined ({1}, account {2})",
        "노드 {0} 참여 ({1}, 계정 {2})",
    ),
    ("node.removed", "node {0} removed", "노드 {0} 제거됨"),
    ("node.online", "node {0} online", "노드 {0} 온라인"),
    (
        "node.paused",
        "node {0} paused: {1}",
        "노드 {0} 일시 중지: {1}",
    ),
    (
        "node.resumed",
        "node {0} takes tasks again",
        "노드 {0} 다시 태스크 받음",
    ),
    ("node.offline", "node {0} offline", "노드 {0} 오프라인"),
    (
        "node.offline.moved",
        "node {0} offline; {1} runs moved back to the queue",
        "노드 {0} 오프라인, 실행 {1}개를 대기열로 되돌림",
    ),
    ("node.asleep", "node {0} went to sleep", "노드 {0} 잠듦"),
    (
        "node.asleep.moved",
        "node {0} went to sleep; {1} runs moved back to the queue",
        "노드 {0} 잠듦, 실행 {1}개를 대기열로 되돌림",
    ),
    (
        "node.wake",
        "magic packet for node {0} sent by {1}",
        "노드 {0}에 매직 패킷 보냄 ({1})",
    ),
    (
        "node.wake_failed",
        "node {0} has not woken after {1} magic packets; check Wake-on-LAN in its firmware and OS settings",
        "노드 {0}: 매직 패킷 {1}번에도 깨어나지 않음. 펌웨어와 OS의 Wake-on-LAN 설정을 확인하세요",
    ),
    (
        "node.suspended",
        "node {0} suspended after {1} failed checks",
        "검증 {1}회 실패로 노드 {0} 정지",
    ),
    (
        "node.executor_unavailable",
        "node {0}: {1} unavailable: {2}",
        "노드 {0}: {1} 사용 불가: {2}",
    ),
    (
        "node.executor_available",
        "node {0}: {1} available",
        "노드 {0}: {1} 사용 가능",
    ),
    (
        "node.trust",
        "{0} now runs trusted jobs",
        "{0}에서 이제 신뢰 작업 실행",
    ),
    (
        "node.untrust",
        "{0} no longer runs trusted jobs",
        "{0}에서 더 이상 신뢰 작업을 실행하지 않음",
    ),
    (
        "upgrade.failed",
        "upgrade of {0} failed: {1}",
        "{0} 업그레이드 실패: {1}",
    ),
    // Jobs and tasks.
    (
        "job.submitted",
        "job {0} ({1}) submitted by {2}: {3} tasks, account {4}",
        "작업 {0} ({1}) 제출 ({2}): 태스크 {3}개, 계정 {4}",
    ),
    (
        "job.submitted.image",
        "job {0} ({1}) submitted by {2}: {3} tasks, account {4}, image {5}",
        "작업 {0} ({1}) 제출 ({2}): 태스크 {3}개, 계정 {4}, 이미지 {5}",
    ),
    (
        "job.succeeded",
        "job {0} ({1}) succeeded: {2} of {3} tasks succeeded",
        "작업 {0} ({1}) 성공: 태스크 {3}개 중 {2}개 성공",
    ),
    (
        "job.cancelled",
        "job {0} ({1}) cancelled: {2} of {3} tasks succeeded",
        "작업 {0} ({1}) 취소됨: 태스크 {3}개 중 {2}개 성공",
    ),
    (
        "job.failed",
        "job {0} ({1}) failed: {2} of {3} tasks succeeded",
        "작업 {0} ({1}) 실패: 태스크 {3}개 중 {2}개 성공",
    ),
    (
        "job.queue_timeout",
        "job {0}: {1} tasks no node took it within the queue timeout of {2}: {3}",
        "작업 {0}: 태스크 {1}개를 대기 시간 제한 {2} 안에 받은 노드 없음: {3}",
    ),
    (
        "job.queue_timeout.one",
        "job {0}: 1 task no node took it within the queue timeout of {1}: {2}",
        "작업 {0}: 태스크 1개를 대기 시간 제한 {1} 안에 받은 노드 없음: {2}",
    ),
    (
        "job.cu_cap",
        "job {0} spent {1}, its account's cap per job ({2}): {3} tasks that had not run fail",
        "작업 {0}이(가) {1}을(를) 써서 계정의 작업당 상한({2})에 닿음: 아직 안 돈 태스크 {3}개 실패",
    ),
    (
        "task.failed",
        "task {0}/{1} failed: {2}",
        "태스크 {0}/{1} 실패: {2}",
    ),
    (
        "task.preempted",
        "task {0}/{1} on {2}: {3}",
        "{2}의 태스크 {0}/{1}: {3}",
    ),
    (
        "task.preempted.grace",
        "task {0}/{1} on {2}: {3}; stopping it ({4} to save its checkpoint)",
        "{2}의 태스크 {0}/{1}: {3}. 멈추는 중 (체크포인트 저장에 {4})",
    ),
    (
        "task.resumed",
        "task {0}/{1} resumes on {2} from its checkpoint ({3})",
        "태스크 {0}/{1}, {2}에서 체크포인트({3})부터 재개",
    ),
    (
        "task.checkpoint",
        "task {0}/{1} saved a checkpoint on {2}: {3} files, {4}",
        "태스크 {0}/{1}, {2}에서 체크포인트 저장: 파일 {3}개, {4}",
    ),
    (
        "task.checkpoint_failed",
        "task {0}/{1} on {2} stopped without a new checkpoint: {3}",
        "{2}의 태스크 {0}/{1}: 새 체크포인트 없이 멈춤: {3}",
    ),
    (
        "task.checkpoint_timeout",
        "task {0}/{1} on {2} did not stop and save its checkpoint in time; cancelled",
        "{2}의 태스크 {0}/{1}: 제때 멈추고 체크포인트를 저장하지 못해 취소함",
    ),
    (
        "task.image",
        "task {0}/{1} on {2} runs {3} ({4})",
        "{2}의 태스크 {0}/{1}: {3} ({4}) 실행",
    ),
    (
        "check.mismatch",
        "check of task {0}/{1} rejected the result of {2}",
        "태스크 {0}/{1} 검증에서 {2}의 결과 거부",
    ),
    (
        "check.inconclusive",
        "check of task {0}/{1} inconclusive: runs on different nodes disagreed",
        "태스크 {0}/{1} 검증 결론 없음: 노드마다 결과가 다름",
    ),
    (
        "perf.drop",
        "benchmark of {0} dropped: {1}",
        "{0} 벤치마크 하락: {1}",
    ),
    // Result checks (1.20).
    ("perf.inflated", "{0}: {1}", "{0}: 부풀린 벤치마크 ({1})"),
    (
        "honeypot.failed",
        "{0} gave a wrong answer to a honeypot task; account {1}'s earnings are held for {2}",
        "{0}이(가) 함정 작업에 틀린 답을 냈습니다. 계정 {1}의 수익을 {2} 동안 보류합니다",
    ),
    // Confidential computing.
    (
        "confidential.keys",
        "keys released to task {0}/{1}",
        "태스크 {0}/{1}에 키 전달",
    ),
    (
        "confidential.refused",
        "keys refused for task {0}/{1}: {2}",
        "태스크 {0}/{1}의 키 거부: {2}",
    ),
    (
        "confidential.measurement",
        "new {0} launch measurement {1}: trust it with `pumasi confidential trust {1}` if this agent's guest is the one you expect",
        "새 {0} 실행 측정값 {1}: 이 에이전트의 게스트가 예상한 것이면 `pumasi confidential trust {1}`로 신뢰하세요",
    ),
    (
        "confidential.trust",
        "{0} launch measurement {1} trusted",
        "{0} 실행 측정값 {1} 신뢰함",
    ),
    (
        "confidential.untrust",
        "{0} launch measurement {1} no longer trusted",
        "{0} 실행 측정값 {1} 더 이상 신뢰하지 않음",
    ),
    // The file store.
    (
        "store.scrub.node",
        "{0} checked {1} replicas ({2}): no damage",
        "{0}에서 복제본 {1}개({2}) 검사: 손상 없음",
    ),
    (
        "store.damaged.node",
        "{0} found damaged data in {1}; fetching it again",
        "{0}에서 {1}의 손상된 데이터 발견, 다시 받는 중",
    ),
    (
        "store.scrub.cp",
        "the control plane checked {0} collections ({1}): no damage",
        "컨트롤 플레인이 컬렉션 {0}개({1}) 검사: 손상 없음",
    ),
    (
        "store.damaged.cp",
        "the control plane's store has damaged data in {0}; node replicas keep good copies",
        "컨트롤 플레인 저장소의 {0}에 손상된 데이터 있음. 노드 복제본에 온전한 사본이 있음",
    ),
    (
        "store.repaired",
        "the control plane fetched {0} back from {1}",
        "컨트롤 플레인이 {1}에서 {0} 복구",
    ),
    (
        "store.missing",
        "the control plane's store lacks {0} dataset versions and no online node has them",
        "컨트롤 플레인 저장소에 데이터셋 버전 {0}개가 없고 온라인 노드에도 없음",
    ),
    // Rollouts.
    (
        "rollout.latest",
        "release {0} downloaded and checked; {1}",
        "릴리스 {0} 다운로드와 확인 완료: {1}",
    ),
    (
        "rollout.latest.failed",
        "upgrade to {0} failed: {1}",
        "{0} 업그레이드 실패: {1}",
    ),
    (
        "rollout.newest.failed",
        "upgrade to the newest release failed: {0}",
        "최신 릴리스 업그레이드 실패: {0}",
    ),
    (
        "rollout.halted",
        "rollout halted at the canaries: {0}",
        "카나리에서 배포 중단: {0}",
    ),
    (
        "rollout.promoted",
        "the canaries passed; the rollout goes on to every node ({0})",
        "카나리 통과, 모든 노드로 배포 계속 ({0})",
    ),
    (
        "rollout.canary",
        "{0} added to the canaries by {1}",
        "{0} 카나리에 추가 ({1})",
    ),
    // Alerts: what fires, and the events around them.
    (
        "alert.node_offline",
        "node {0} offline for {1}",
        "노드 {0} {1} 동안 오프라인",
    ),
    (
        "alert.disk",
        "disk of {0} {1}% full ({2} of {3})",
        "{0} 디스크 {1}% 사용 ({3} 중 {2})",
    ),
    (
        "alert.cp_disk",
        "control plane disk (file store) {0}% full ({1} of {2})",
        "컨트롤 플레인 디스크(파일 저장소) {0}% 사용 ({2} 중 {1})",
    ),
    ("alert.temperature", "{0} is at {1} °C", "{0} 온도 {1} °C"),
    (
        "alert.smart.failed",
        "drive {0} of {1} fails its SMART health check",
        "{1}의 드라이브 {0} SMART 상태 검사 실패",
    ),
    (
        "alert.smart.worn",
        "drive {0} of {1} has used {2}% of its endurance",
        "{1}의 드라이브 {0} 수명 {2}% 사용",
    ),
    (
        "alert.smart.errors",
        "drive {0} of {1} reports {2} media errors",
        "{1}의 드라이브 {0} 미디어 오류 {2}건",
    ),
    (
        "alert.failure_rate",
        "{0} of {1} runs failed in the last {2} ({3}%)",
        "최근 {2} 동안 실행 {1}개 중 {0}개 실패 ({3}%)",
    ),
    (
        "alert.silenced_firing",
        "{inner} (silenced)",
        "{inner} (알림 중지됨)",
    ),
    ("alert.resolved", "resolved: {inner}", "해결됨: {inner}"),
    (
        "alert.silenced",
        "{inner} silenced for {0} by {1}",
        "{inner} 알림 {0} 동안 중지 ({1})",
    ),
    (
        "alert.silenced.comment",
        "{inner} silenced for {0} by {1}: {2}",
        "{inner} 알림 {0} 동안 중지 ({1}): {2}",
    ),
    (
        "alert.unsilenced",
        "silence of {inner} ended",
        "{inner} 알림 중지 끝남",
    ),
    // What a silence covers.
    ("silence.all", "every alert", "모든 경보"),
    ("silence.all_on", "every alert on {0}", "{0}의 모든 경보"),
    ("silence.rule", "{0}", "{0}"),
    ("silence.rule_on", "{0} on {1}", "{1}의 {0}"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_both_languages() {
        let t = Text::new("node.offline.moved", ["imac", "3"]);
        assert_eq!(t.en(), "node imac offline; 3 runs moved back to the queue");
        assert_eq!(
            t.render(true).unwrap(),
            "노드 imac 오프라인, 실행 3개를 대기열로 되돌림"
        );
        let r = Text::wrap(
            "alert.resolved",
            [""; 0],
            Text::new("alert.temperature", ["imac", "91"]),
        );
        assert_eq!(r.en(), "resolved: imac is at 91 °C");
        assert_eq!(r.render(true).unwrap(), "해결됨: imac 온도 91 °C");
        // Unknown keys and missing arguments render nothing.
        assert_eq!(Text::new("nope", ["x"]).render(true), None);
        assert_eq!(Text::new("node.paused", ["imac"]).render(false), None);
        assert_eq!(Text::new("nope", ["x"]).en(), "nope");
    }

    #[test]
    fn every_template_is_sound() {
        let mut keys = std::collections::HashSet::new();
        for (k, en, ko) in TEXTS {
            assert!(keys.insert(*k), "{k} twice");
            // Both languages use the same arguments.
            let args = |s: &str| {
                let mut v: Vec<String> = s
                    .split('{')
                    .skip(1)
                    .map(|p| p.split('}').next().unwrap().to_string())
                    .collect();
                v.sort();
                v.dedup();
                v
            };
            assert_eq!(args(en), args(ko), "{k}");
            for a in args(en) {
                assert!(a == "inner" || a.parse::<usize>().is_ok(), "{k}: {{{a}}}");
            }
        }
    }
}
