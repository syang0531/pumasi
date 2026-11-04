# Pumasi (품앗이)

이미 가진 여러 대의 기기를 하나의 사설 클러스터로 묶어 배치 작업을 나눠 돌립니다. 모든 기기에서 도는 안전한 WebAssembly 작업과, 허용한 기기에서 도는 일반 Docker 이미지 작업(Python, 원하는 패키지, 선택적 네트워크)을 같은 방식으로 실행합니다. Linux, macOS, Windows와 x86_64, ARM64에서 같은 Rust 단일 바이너리 하나로 동작합니다.

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

## 명령어

| 명령 | 설명 |
| --- | --- |
| `pumasi cluster start` | 컨트롤 플레인 실행. 처음 실행하면 클러스터를 만들고, 상태는 `~/.pumasi/cluster`에 저장합니다. |
| `pumasi token [--ttl 24h] [--uses N]` | 다른 기기용 참가 티켓 발급 |
| `pumasi agent join <티켓> [--allow-docker]` | 클러스터에 참가하고 작업 실행 시작. `--allow-docker`면 Docker 작업도 받음 |
| `pumasi agent start [--allow-docker]` | 이전에 참가한 클러스터에 티켓 없이 재접속 |
| `pumasi run job.toml [--output] [--out DIR]` | 작업 제출 후 완료까지 대기. 결과 파일은 `DIR`(기본 `pumasi-out/<작업 ID>`)에 저장. 성공 시 종료 코드 0, 실패 태스크가 있으면 2 |
| `pumasi status` | 노드(온라인 여부, 실행기, 슬롯, 실행 중)와 최근 작업 |
| `pumasi job <ID> [--output] [--out DIR]` | 태스크별 상태, 노드, 시도 횟수, 시간, 출력. `--out`이면 결과 파일 다운로드 |
| `pumasi cancel <ID>` | 작업 취소 |
| `pumasi remove-node <ID>` | 노드 퇴출(다시 오려면 새 티켓 필요) |
| `pumasi login <관리자 티켓>` | 다른 기기에서 원격 클러스터 관리 |
| `pumasi exec module.wasm [--input DIR] [인자...]` | 에이전트와 같은 샌드박스로 로컬 실행 (`DIR`이 `/input`으로 보이고 `/output`은 `pumasi-out/exec`에 저장) |

`--home DIR` 또는 `PUMASI_HOME`으로 상태 디렉터리를 바꾸면 한 기기에서 클러스터와 에이전트 여러 개를 띄워 시험할 수 있습니다.

## 작업 명세 (job.toml)

작업은 두 실행기 중 하나에서 돌고, 명세 형식과 명령은 같습니다.

| | `wasm` (기본) | `docker` |
| --- | --- | --- |
| 실행 대상 | `wasm32-wasip1` 모듈 | Linux OCI 이미지 |
| 실행 위치 | 모든 노드 | `--allow-docker`로 시작한 노드만 |
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

`shards` 대신 `[[task]]`로 태스크를 하나씩 나열할 수도 있고, 태스크마다 `files = [...]`로 그 태스크만 받는 파일을 줄 수 있습니다.

컨테이너는 `network = true`가 아니면 `--network none`으로, 메모리·CPU·프로세스 수 상한과 `no-new-privileges`를 걸고, 호스트 폴더를 마운트하지 않은 채(파일은 복사로 주고받음) 실행됩니다. 이미지는 처음 쓸 때 받아 옵니다.

## 보안 모델 (1단계)

- 참가 티켓은 만료 시간과 사용 횟수를 가진 비밀값입니다. 한 번 참가한 노드는 공개키로 기억되어 다시 티켓이 필요 없고, `remove-node`로 퇴출합니다.
- `~/.pumasi/cluster/admin.ticket`은 전체 권한이므로 비밀번호처럼 다뤄야 합니다.
- WASM 태스크는 실행 기기로부터 격리(자기 `/input`·`/output`만 보임, 네트워크 차단, 메모리·CPU 상한)됩니다.
- Docker 태스크는 컨테이너 수준의 격리라 호스트 커널을 공유합니다. 그래서 기기 주인이 `--allow-docker`를 줘야만 실행합니다. 내 기기나 믿을 수 있는 사람끼리의 클러스터에서 쓰고, 모르는 사람의 작업은 3단계 microVM 실행기가 맡습니다.
- 어느 쪽이든 기기 주인에게 내용이 숨겨지지는 않습니다. 1단계는 "오픈" 등급만 있습니다.

## 로드맵

| 단계 | 범위 | 상태 |
| --- | --- | --- |
| 1 | 사설 클러스터: 에이전트, 컨트롤 플레인, iroh 연결, WASM·Docker 실행기, 파일 입출력, CLI | 이번 릴리스 |
| 2 | 초대한 외부 제공자(오픈 등급), 노드·클러스터 공동 서명 사용 영수증, 고정 요금표, 선불 원장 | 다음 |
| 3 | microVM 실행기(Docker 작업과 같은 OCI 이미지를 VM 수준 격리로, Linux GPU 패스스루), 같은 작업 명세 유지 | 예정 |
| 4 | SEV-SNP·TDX 서버 기반 기밀 등급과 원격 증명 | 예정 |

## 라이선스

Apache-2.0
