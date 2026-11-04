# Pumasi (품앗이)

이미 가진 여러 대의 기기를 하나의 사설 클러스터로 묶어 배치 작업을 나눠 돌립니다. 모든 기기에서 도는 안전한 WebAssembly 작업과, 허용한 기기에서 도는 일반 Docker 이미지 작업(Python, 원하는 패키지, 선택적 네트워크)을 같은 방식으로 실행합니다. Linux, macOS, Windows와 x86_64, ARM64에서 같은 Rust 단일 바이너리 하나로 동작합니다.

단계별로 넓혀 갑니다. 1단계는 혼자 써도 쓸모 있는 사설 클러스터 도구, 2단계(이번 릴리스)는 초대한 다른 사람의 기기를 제공자로 받고 서명된 사용 영수증, 선불 크레딧 원장, 결과 표본 검증을 더한 것이고, 이후 같은 작업 명세로 남의 유휴 장비까지 빌려 쓰는 마켓으로 넓힙니다. 구조는 [docs/architecture.md](docs/architecture.md)에 있습니다.

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

### Docker 작업: Python, 파일, 그 밖의 무엇이든

컨테이너를 돌려도 되는 기기의 에이전트를 `--allow-docker`로 시작합니다(그 기기에 Docker가 설치되어 있어야 합니다).

```sh
pumasi agent join pumasijoin... --allow-docker
```

Python 단어 세기 샘플은 스크립트와 텍스트 파일 폴더를 컨테이너 4개에 나눠 보내고, 각 컨테이너가 `/output`에 쓴 결과를 내 컴퓨터로 가져옵니다.

```sh
pumasi run examples/python-wordcount/job.toml --output
python3 examples/python-wordcount/merge.py pumasi-out/<작업 ID>
```

### 다른 사람의 기기: 초대, 크레딧, 검증

친구가 자기 PC를 내 클러스터에 빌려줄 수 있습니다. 그 사람의 계정 이름으로 초대합니다.

```sh
pumasi invite bob                  # bob이 자기 기기에서 실행할 `pumasi agent join pumasijoin...` 출력
pumasi credit add owner 100        # 남의 기기에서 돌린 작업에 쓸 선불 크레딧
pumasi run examples/montecarlo/job.toml
```

- bob의 기기는 **제공자**로 참가합니다. WASM 작업만 받고(Docker는 초대할 때 `--allow-docker`를 주고 bob도 에이전트를 `--allow-docker`로 띄운 경우에만), 크레딧이 있는 계정의 작업만 받습니다.
- 내 기기를 먼저 쓰고 비용은 없습니다. 넘치는 작업이 제공자에게 갑니다. 제공자 실행은 고정 요금표(`pumasi prices`)로 값을 매겨 작업 계정에서 차감하고, 제공자 수익은 **보류**(기본 1시간)했다가 사용 가능 잔액으로 옮깁니다.
- 실행마다 **사용 영수증**(fuel, 실행 시간, 메모리, 실행기, 결과 해시)이 나오고, 노드가 서명하고 컨트롤 플레인이 맞서명합니다. `pumasi receipts <작업 ID> --export r.json`으로 내보내고 `pumasi verify-receipts r.json`으로 오프라인 검증합니다.
- WASM 작업은 기본으로 **결정적 모드**(가상 시계, 시드 고정 난수)로 돌아 정직한 노드라면 같은 결과를 냅니다. 제공자가 돌린 태스크 일부(10%, 새 제공자는 50%, 검증에 한 번 걸린 제공자는 전부)를 **다른 노드에서 다시 돌려** 비교하고, 가능하면 내 기기에서 돌립니다. 결과가 다르면 그 수익은 몰수(작업 계정에 환불)되고 스트라이크가 쌓이며, 3번이면 더는 작업을 받지 못합니다. 검증 실행의 결과가 맞으면 그 결과로 바꿔 줍니다.

크레딧은 장부일 뿐 실제 돈은 오가지 않습니다. `pumasi credit payout bob 10`은 Pumasi 밖에서 bob에게 정산했다는 기록입니다.

## 명령어

| 명령 | 설명 |
| --- | --- |
| `pumasi cluster start` | 컨트롤 플레인 실행. 처음 실행하면 클러스터를 만들고, 상태는 `~/.pumasi/cluster`에 저장합니다. |
| `pumasi token [--ttl 24h] [--uses N]` | 내 다른 기기용 참가 티켓 발급 |
| `pumasi invite <계정> [--ttl 7d] [--uses 1] [--allow-docker]` | 다른 사람의 기기를 `계정` 몫으로 버는 제공자로 초대 |
| `pumasi agent join <티켓> [--allow-docker]` | 클러스터에 참가하고 작업 실행 시작. `--allow-docker`면 Docker 작업도 받음 |
| `pumasi agent start [--allow-docker]` | 이전에 참가한 클러스터에 티켓 없이 재접속 |
| `pumasi run job.toml [--account A] [--output] [--out DIR]` | 작업 제출 후 완료까지 대기. 결과 파일은 `DIR`(기본 `pumasi-out/<작업 ID>`)에 저장. 성공 시 종료 코드 0, 실패 태스크가 있으면 2. `--account`는 제공자 실행 비용을 낼 계정(기본 `owner`) |
| `pumasi status` | 노드(역할, 온라인 여부, 실행기, 슬롯, 실행 중, 검증 기록)와 최근 작업 및 비용 |
| `pumasi job <ID> [--output] [--out DIR]` | 태스크별 상태, 노드, 시도 횟수, 시간, 비용, 검증 결과, 출력. `--out`이면 결과 파일 다운로드 |
| `pumasi credit list \| add <계정> <금액> \| payout <계정> <금액> \| ledger` | 잔액(사용 가능, 보류, 지출, 수익), 선불 충전, 지급 기록, 원장 |
| `pumasi prices [set ...]` | 요금표와 정산 정책(보류 시간, 검증 비율, 최대 스트라이크) 조회·변경 |
| `pumasi receipts <ID> [--export FILE]` | 작업의 서명된 사용 영수증 |
| `pumasi verify-receipts FILE [--cluster ID]` | 내보낸 영수증을 오프라인으로 검증 |
| `pumasi cancel <ID>` | 작업 취소 |
| `pumasi remove-node <ID>` | 노드 퇴출(다시 오려면 새 티켓 필요) |
| `pumasi login <관리자 티켓>` | 다른 기기에서 원격 클러스터 관리 |
| `pumasi exec module.wasm [--input DIR] [--seed S] [인자...]` | 에이전트와 같은 샌드박스로 로컬 실행 (`DIR`이 `/input`으로 보이고 `/output`은 `pumasi-out/exec`에 저장, `--seed`는 결정적 모드) |

`--home DIR` 또는 `PUMASI_HOME`으로 상태 디렉터리를 바꾸면 한 기기에서 클러스터와 에이전트 여러 개를 띄워 시험할 수 있습니다.

## 작업 명세 (job.toml)

작업은 두 실행기 중 하나에서 돌고, 명세 형식과 명령은 같습니다.

| | `wasm` (기본) | `docker` |
| --- | --- | --- |
| 실행 대상 | `wasm32-wasip1` 모듈 | Linux OCI 이미지 |
| 실행 위치 | 모든 노드 | `--allow-docker`로 시작한 노드만 (제공자는 초대에도 `--allow-docker` 필요) |
| 격리 | Wasmtime 샌드박스, 모르는 코드도 안전 | 컨테이너, 내 기기나 믿는 사람끼리만 |
| 네트워크 | 불가 | 기본 차단, `network = true`로 허용 |
| 파일 | `/input`(읽기 전용), `/output` | `/input`(읽기 전용), `/output` |

WASM 작업:

```toml
name = "estimate-pi"
module = "montecarlo.wasm"            # wasm32-wasip1 모듈
shards = 16                           # 태스크 N개, {shard}·{shards} 치환
args = ["sample", "{shard}", "5000000"]
files = ["data/"]                     # 선택, /input 아래에 보임
deterministic = true                  # 기본값: 가상 시계 + 시드 고정 난수, 결과 검증 가능

[resources]
memory_mb = 32                        # 태스크당 메모리 상한 (기본 256)
timeout = "2m"                        # 태스크당 시간 상한 (기본 5분)
fuel = 10_000_000_000                 # 명령 수 예산 (wasm 전용, 기본 무제한)
```

Docker 작업:

```toml
name = "wordcount"
image = "python:3.12-slim"            # image를 쓰면 docker 실행기
command = ["python", "/input/count.py", "{shard}", "{shards}"]
files = ["count.py", "texts/"]        # 모든 태스크의 /input에 복사
shards = 4
network = false                       # 기본값. true면 외부 네트워크 허용
# gpus = "all"                        # docker run --gpus 에 전달

[resources]
memory_mb = 512
cpus = 1.5                            # 태스크당 CPU 상한 (docker 전용)
timeout = "10m"
```

`files`의 경로는 job.toml 기준이고, 폴더는 하위까지 그대로 `/input` 아래에 복사됩니다. 한 작업의 입력 파일은 합쳐서 48 MiB까지입니다. 태스크가 `/output`에 쓴 파일(태스크당 32 MiB까지)은 컨트롤 플레인에 저장되고, `pumasi run`이 `pumasi-out/<작업 ID>/task-<N>/`로 내려받습니다. stdout·stderr는 각각 4 MiB까지 보관합니다.

결정적 모드(WASM 작업 기본값)에서는 시계가 2000-01-01에서 시작해 읽을 때마다 1ms씩 가고, 난수는 작업과 태스크 번호로 만든 시드에서 나옵니다. 실제 시간이나 새 난수가 필요한 작업은 `deterministic = false`로 두고, 그러면 제공자 실행은 검증하지 않습니다.

`shards` 대신 `[[task]]`로 태스크를 하나씩 나열할 수도 있고, 태스크마다 `files = [...]`로 그 태스크만 받는 파일을 줄 수 있습니다.

컨테이너는 `network = true`가 아니면 `--network none`으로, 메모리·CPU·프로세스 수 상한과 `no-new-privileges`를 걸고, 호스트 폴더를 마운트하지 않은 채(파일은 복사로 주고받음) 실행됩니다. 이미지는 처음 쓸 때 받아 옵니다.

## 보안 모델

- 참가 티켓은 만료 시간과 사용 횟수를 가진 비밀값입니다. 한 번 참가한 노드는 공개키로 기억되어 다시 티켓이 필요 없고, `remove-node`로 퇴출합니다.
- `~/.pumasi/cluster/admin.ticket`은 전체 권한이므로 비밀번호처럼 다뤄야 합니다.
- WASM 태스크는 실행 기기로부터 격리(자기 `/input`·`/output`만 보임, 네트워크 차단, 메모리·CPU 상한)됩니다.
- Docker 태스크는 컨테이너 수준의 격리라 호스트 커널을 공유합니다. 그래서 기기 주인이 `--allow-docker`를 줘야만 실행합니다. 내 기기나 믿을 수 있는 사람끼리의 클러스터에서 쓰고, 모르는 사람의 작업은 3단계 microVM 실행기가 맡습니다.
- 제공자 기기(`pumasi invite`로 받은 남의 기기)는 초대가 허용하지 않으면 WASM만 받고, 검증의 판정은 내 기기만 맡습니다.
- 제공자는 가짜 결과를 낼 수 있습니다. 서명은 누가 무엇을 주장했는지를 증명할 뿐 참임을 증명하지는 않습니다. 표본 검증이 확률적으로 잡습니다. 기본 10%면 가짜 결과 20개를 낸 제공자는 88% 확률로 걸리고, 걸린 수익은 몰수되며, 3번이면 정지됩니다. 수익을 1시간 보류하는 이유가 이것입니다.
- 어느 쪽이든 기기 주인에게 내용이 숨겨지지는 않습니다. 지금은 "오픈" 등급만 있습니다.

## 로드맵

| 단계 | 범위 | 상태 |
| --- | --- | --- |
| 1 | 사설 클러스터: 에이전트, 컨트롤 플레인, iroh 연결, WASM·Docker 실행기, 파일 입출력, CLI | 완료 |
| 2 | 초대한 외부 제공자(오픈 등급), 노드·클러스터 공동 서명 사용 영수증, 고정 요금표, 선불 크레딧 원장, 표본 재실행 검증 | 이번 릴리스 |
| 3 | microVM 실행기(Docker 작업과 같은 OCI 이미지를 VM 수준 격리로, Linux GPU 패스스루), 같은 작업 명세 유지 | 예정 |
| 4 | SEV-SNP·TDX 서버 기반 기밀 등급과 원격 증명 | 예정 |

## 라이선스

Apache-2.0
