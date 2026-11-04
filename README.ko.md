# Pumasi (품앗이)

이미 가진 여러 대의 기기를 하나의 사설 클러스터로 묶어 WebAssembly 배치 작업을 나눠 돌립니다. Linux, macOS, Windows와 x86_64, ARM64에서 같은 Rust 단일 바이너리 하나로 동작합니다.

이 저장소는 전체 계획의 1단계입니다. 먼저 혼자 써도 쓸모 있는 사설 클러스터 도구를 완성하고, 이후 초대한 외부 제공자, 사용량 과금, 같은 작업 명세로 남의 유휴 장비까지 빌려 쓰는 마켓으로 넓힙니다. 구조는 [docs/architecture.md](docs/architecture.md)에 있습니다.

## 빠른 시작

```sh
cargo build --release -p pumasi-cli          # target/release/pumasi 생성

# 1번 기기: 컨트롤 플레인 시작. 참가 명령이 출력됩니다.
pumasi cluster start --name homelab

# 2~N번 기기: 출력된 참가 명령을 붙여 넣습니다.
pumasi agent join pumasijoin...

# 관리자 티켓이 있는 곳(기본은 1번 기기)에서:
pumasi status
```

샘플 작업(몬테카를로로 원주율 추정, 16개 샤드):

```sh
rustup target add wasm32-wasip1
cargo build --release --target wasm32-wasip1 --manifest-path examples/montecarlo/Cargo.toml
pumasi run examples/montecarlo/job.toml --output
```

## 명령어

| 명령 | 설명 |
| --- | --- |
| `pumasi cluster start` | 컨트롤 플레인 실행. 처음 실행하면 클러스터를 만들고, 상태는 `~/.pumasi/cluster`에 저장합니다. |
| `pumasi token [--ttl 24h] [--uses N]` | 다른 기기용 참가 티켓 발급 |
| `pumasi agent join <티켓>` | 클러스터에 참가하고 작업 실행 시작 |
| `pumasi agent start` | 이전에 참가한 클러스터에 티켓 없이 재접속 |
| `pumasi run job.toml [--output]` | 작업 제출 후 완료까지 대기. 성공 시 종료 코드 0, 실패 태스크가 있으면 2 |
| `pumasi status` | 노드(온라인 여부, 슬롯, 실행 중)와 최근 작업 |
| `pumasi job <ID> [--output]` | 태스크별 상태, 노드, 시도 횟수, 시간, 출력 |
| `pumasi cancel <ID>` | 작업 취소 |
| `pumasi remove-node <ID>` | 노드 퇴출(다시 오려면 새 티켓 필요) |
| `pumasi login <관리자 티켓>` | 다른 기기에서 원격 클러스터 관리 |
| `pumasi exec module.wasm [인자...]` | 에이전트와 같은 샌드박스로 로컬 실행 |

`--home DIR` 또는 `PUMASI_HOME`으로 상태 디렉터리를 바꾸면 한 기기에서 클러스터와 에이전트 여러 개를 띄워 시험할 수 있습니다.

## 작업 명세 (job.toml)

```toml
name = "estimate-pi"
module = "montecarlo.wasm"            # wasm32-wasip1 모듈
shards = 16                           # 태스크 N개, {shard}·{shards} 치환
args = ["sample", "{shard}", "5000000"]

[resources]
memory_mb = 32                        # 태스크당 메모리 상한 (기본 256)
timeout = "2m"                        # 태스크당 시간 상한 (기본 5분)
fuel = 10_000_000_000                 # 명령 수 예산 (기본 무제한)
```

`shards` 대신 `[[task]]`로 태스크를 하나씩 나열할 수도 있습니다. 태스크는 stdin을 읽고 stdout에 쓰며, 파일시스템과 네트워크는 쓸 수 없습니다.

## 보안 모델 (1단계)

- 참가 티켓은 만료 시간과 사용 횟수를 가진 비밀값입니다. 한 번 참가한 노드는 공개키로 기억되어 다시 티켓이 필요 없고, `remove-node`로 퇴출합니다.
- `~/.pumasi/cluster/admin.ticket`은 전체 권한이므로 비밀번호처럼 다뤄야 합니다.
- 태스크는 실행 기기로부터 격리(파일·네트워크 차단, 메모리·CPU 상한)되지만, 기기 주인에게 내용이 숨겨지지는 않습니다. 1단계는 "오픈" 등급만 있습니다.

## 로드맵

| 단계 | 범위 | 상태 |
| --- | --- | --- |
| 1 | 사설 클러스터: 에이전트, 컨트롤 플레인, iroh 연결, WASM 실행기, CLI | 이번 릴리스 |
| 2 | 초대한 외부 제공자(오픈 등급), 노드·클러스터 공동 서명 사용 영수증, 고정 요금표, 선불 원장 | 다음 |
| 3 | microVM 실행기(OCI 이미지, Linux GPU 패스스루), 같은 작업 명세 유지 | 예정 |
| 4 | SEV-SNP·TDX 서버 기반 기밀 등급과 원격 증명 | 예정 |

## 라이선스

Apache-2.0
