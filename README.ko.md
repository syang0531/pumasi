# Pumasi (품앗이)

이미 가진 여러 대의 기기를 하나의 사설 클러스터로 묶어 배치 작업을 나눠 돌립니다. 모든 기기에서 도는 안전한 WebAssembly 작업, 허용한 기기에서 도는 일반 Docker 이미지 작업(Python, 원하는 패키지, 선택적 네트워크), 그리고 같은 이미지를 태스크마다 자기 커널을 가진 microVM 안에서 돌리는 작업(남이 빌려준 기기에도 보낼 수 있을 만큼 격리됨)을 같은 방식으로 실행합니다. Linux, macOS, Windows와 x86_64, ARM64에서 같은 Rust 단일 바이너리 하나로 동작합니다.

단계별로 넓혀 갑니다. 1단계는 혼자 써도 쓸모 있는 사설 클러스터 도구, 2단계는 초대한 다른 사람의 기기를 제공자로 받고 서명된 사용 영수증, 선불 크레딧 원장, 결과 표본 검증을 더한 것, 3단계는 그 기기들에서도 컨테이너 이미지를 VM 격리로 돌리는 microVM 실행기, 4단계는 릴리스 바이너리, OS 서비스 설치, 클러스터 키로 서명한 일괄 업그레이드, 5단계(이번 릴리스)는 세 실행기 모두를 Windows에서 정식 지원하는 것입니다. 이후 구형 Mac, 저장소, GPU, 클러스터 관리, 모니터링, 대시보드를 거쳐 마켓으로 넓힙니다. 구조는 [docs/architecture.md](docs/architecture.md)에 있습니다.

## 설치

릴리스마다 Linux(x86_64, aarch64, 정적 링크), macOS(x86_64는 10.15부터, Apple Silicon), Windows(x86_64, ARM64)용 `pumasi` 바이너리와 `SHA256SUMS`, 그리고 맞는 바이너리를 받아 체크섬까지 확인하는 설치 스크립트 두 개가 올라갑니다.

```sh
# Linux, macOS: root면 /usr/local/bin, 아니면 ~/.local/bin에 설치
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sh
```

```powershell
# Windows: 관리자면 Program Files\Pumasi, 아니면 %LOCALAPPDATA%에 설치하고 PATH에 추가
irm https://github.com/syang0531/pumasi/releases/latest/download/install.ps1 | iex
```

옵션: `--version v0.4.0` / `-Version`, `--dir` / `-Dir`. 참가 티켓을 주면 에이전트를 서비스로도 설치합니다: `curl ... | sudo sh -s -- --ticket pumasijoin... -- --allow-microvm`, 또는 `& ([scriptblock]::Create((irm .../install.ps1))) -Ticket pumasijoin...`. 저장소가 비공개인 동안에는 읽기 권한이 있는 토큰을 `GITHUB_TOKEN`에 넣으면 GitHub API로 받습니다. 소스에서 빌드하려면 `cargo build --release -p pumasi-cli`.

## 빠른 시작

```sh
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

컨테이너를 돌려도 되는 기기의 에이전트를 `--allow-docker`로 시작합니다(그 기기에 Docker가 설치되어 있어야 합니다. Windows는 WSL 2 백엔드에 Linux 컨테이너 모드인 Docker Desktop).

```sh
pumasi agent join pumasijoin... --allow-docker
```

Python 단어 세기 샘플은 스크립트와 텍스트 파일 폴더를 컨테이너 4개에 나눠 보내고, 각 컨테이너가 `/output`에 쓴 결과를 내 컴퓨터로 가져옵니다.

```sh
pumasi run examples/python-wordcount/job.toml --output
python3 examples/python-wordcount/merge.py pumasi-out/<작업 ID>
```

### microVM 작업: 내 것이 아닌 기기에서 컨테이너 돌리기

컨테이너는 호스트 커널을 공유하므로 Docker 작업은 내 기기용입니다. microVM 실행기는 같은 OCI 이미지를 태스크마다 가벼운 VM(KVM이 있는 Linux는 [Firecracker](https://firecracker-microvm.github.io/), Windows는 WHPX 가속 QEMU, 그 밖에는 QEMU) 안에서 자기 Linux 커널로, 네트워크 없이 돌립니다. 작업 명세에는 `executor = "microvm"` 한 줄만 더합니다.

```sh
# 기기마다 한 번 (KVM이 있는 Linux, Windows 10/11 x86_64. Windows는 QEMU를 설치하므로 관리자 터미널에서)
pumasi vm setup
pumasi vm run python:3.12-slim -- python -c 'import platform; print(platform.release())'
# 6.1.102   <- 호스트가 아니라 게스트 커널

pumasi agent join pumasijoin... --allow-microvm
pumasi run examples/python-wordcount/job-microvm.toml --output
```

`pumasi vm setup`은 버전이 고정된 게스트 커널(Linux는 Firecracker, Windows는 QEMU도)을 받아 SHA-256을 확인하고, `pumasi vm status`는 설치 상태와 쓸 VM 모니터를 보여 줍니다. 릴리스 바이너리에는 게스트 init(VM의 PID 1)이 들어 있고, 소스 빌드는 `--guest`로 정적 Linux 빌드(`cargo build --release -p pumasi-guest --target x86_64-unknown-linux-musl`)를 넘깁니다. 에이전트가 이미지를 직접 받고(Docker 불필요) VM 디스크도 직접 포맷하며(e2fsprogs 불필요) 이미지는 VM 안에서 풀기 때문에, 믿을 수 없는 이미지 레이어가 호스트 파일시스템에 닿지 않습니다.

### Windows

Windows 10·11(x86_64)에서 모두 동작합니다. WASM은 그대로, Docker는 Docker Desktop으로, microVM은 Windows 하이퍼바이저 플랫폼(WHPX)을 쓰는 QEMU로 돌립니다. 관리자 PowerShell에서:

```powershell
# 한 번만: QEMU가 쓰는 하이퍼바이저 API를 켜고 재부팅
Enable-WindowsOptionalFeature -Online -FeatureName HypervisorPlatform

pumasi vm setup        # 게스트 커널과 버전 고정 QEMU (~\.pumasi\vm\qemu에 설치)
pumasi vm status       # "whpx usable"이면 하드웨어 가속 microVM
pumasi service install --ticket pumasijoin... -- --allow-microvm --allow-docker
```

- WHPX가 없으면(기능이 꺼졌거나 펌웨어에서 가상화가 꺼짐) microVM은 QEMU 소프트웨어 에뮬레이션으로 돕니다. 결과는 같고 몇 배 느립니다. 비교하려면 `PUMASI_QEMU_ACCEL=tcg`로 강제할 수 있습니다. WHPX가 있어도 클라우드 VM처럼 중첩된 환경에서는 VM 디스크가 깨지는데, 에이전트가 시작할 때 작은 프로브 VM을 띄워 보고 그러면 자동으로 에뮬레이션으로 돌리며 로그에 남깁니다.
- Docker 작업에는 WSL 2 백엔드, Linux 컨테이너 모드로 실행 중인 Docker Desktop이 필요합니다. Docker Desktop은 로그인한 사용자와 함께 시작하므로, 아무도 로그인하지 않은 기기는 WASM과 microVM 작업만 받습니다(Docker Desktop 로그인 시 시작과 자동 로그인을 켜면 해결).
- `pumasi service install`은 사설·도메인 네트워크에서 에이전트가 UDP를 받도록 방화벽 규칙을 추가해, 릴레이 대신 직접 연결되게 합니다. `service uninstall`이 규칙을 지웁니다. Windows가 공용(Public)으로 분류한 네트워크에서는 릴레이로 연결되며, 넓히려면 `netsh advfirewall firewall set rule name="Pumasi agent (UDP in)" new profile=any`.
- 에이전트가 띄운 VM과 Docker CLI는 Windows Job Object에 묶여, 에이전트가 비정상 종료해도 함께 끝납니다.

### 다른 사람의 기기: 초대, 크레딧, 검증

친구가 자기 PC를 내 클러스터에 빌려줄 수 있습니다. 그 사람의 계정 이름으로 초대합니다.

```sh
pumasi invite bob                  # bob이 자기 기기에서 실행할 `pumasi agent join pumasijoin...` 출력
pumasi credit add owner 100        # 남의 기기에서 돌린 작업에 쓸 선불 크레딧
pumasi run examples/montecarlo/job.toml
```

- bob의 기기는 **제공자**로 참가합니다. WASM 작업과, bob이 에이전트를 `--allow-microvm`으로 띄웠다면 microVM 작업을 받고(Docker는 초대할 때 `--allow-docker`를 주고 bob도 에이전트를 `--allow-docker`로 띄운 경우에만), 크레딧이 있는 계정의 작업만 받습니다.
- 내 기기를 먼저 쓰고 비용은 없습니다. 넘치는 작업이 제공자에게 갑니다. 제공자 실행은 고정 요금표(`pumasi prices`)로 값을 매겨 작업 계정에서 차감하고, 제공자 수익은 **보류**(기본 1시간)했다가 사용 가능 잔액으로 옮깁니다.
- 실행마다 **사용 영수증**(fuel, 실행 시간, 메모리, 실행기, 결과 해시)이 나오고, 노드가 서명하고 컨트롤 플레인이 맞서명합니다. `pumasi receipts <작업 ID> --export r.json`으로 내보내고 `pumasi verify-receipts r.json`으로 오프라인 검증합니다.
- WASM 작업은 기본으로 **결정적 모드**(가상 시계, 시드 고정 난수)로 돌아 정직한 노드라면 같은 결과를 냅니다. 제공자가 돌린 태스크 일부(10%, 새 제공자는 50%, 검증에 한 번 걸린 제공자는 전부)를 **다른 노드에서 다시 돌려** 비교하고, 가능하면 내 기기에서 돌립니다. 결과가 다르면 그 수익은 몰수(작업 계정에 환불)되고 스트라이크가 쌓이며, 3번이면 더는 작업을 받지 못합니다. 검증 실행의 결과가 맞으면 그 결과로 바꿔 줍니다.

크레딧은 장부일 뿐 실제 돈은 오가지 않습니다. `pumasi credit payout bob 10`은 Pumasi 밖에서 bob에게 정산했다는 기록입니다.

### 에이전트를 서비스로 실행

`pumasi agent join`은 터미널을 차지하고 돕니다. 재부팅해도 클러스터에 다시 붙게 하려면 서비스로 설치합니다.

```sh
sudo pumasi service install --ticket pumasijoin... -- --allow-microvm
pumasi service status
```

| OS | root·관리자로 설치 | 일반 사용자로 설치 |
| --- | --- | --- |
| Linux | systemd 유닛 `/etc/systemd/system/pumasi-agent.service`, 부팅 시 시작 | `systemctl --user` 유닛, lingering을 켜서 부팅 시 시작 |
| macOS | LaunchDaemon `/Library/LaunchDaemons/pumasi.agent.plist`, 부팅 시 시작 | LaunchAgent, 로그인 시 시작 |
| Windows | Windows 서비스 `pumasi-agent` (자동 시작, 실패 시 재시작) | 관리자 PowerShell 필요 |

`--ticket`은 첫 시작 때 클러스터에 참가하고, 없으면 이미 참가한 기기여야 합니다. `--` 뒤의 옵션은 `pumasi agent start`로 넘어갑니다. 로그는 Linux는 journal, macOS와 Windows는 `~/.pumasi/logs/agent.log`입니다. `pumasi service uninstall`은 서비스만 지우고 멤버십은 남깁니다(퇴출은 `pumasi remove-node`).

### 모든 에이전트를 한 번에 업그레이드

새 릴리스의 바이너리를 받아 관리자 티켓이 있는 곳에서 배포합니다.

```sh
pumasi upgrade rollout pumasi-linux-x86_64 pumasi-macos-aarch64 pumasi-windows-x86_64.exe --wait
pumasi upgrade status
```

플랫폼과 버전은 파일에서 읽습니다. 컨트롤 플레인이 바이너리마다 클러스터 키로 서명하고, 에이전트는 자기가 참가한 클러스터가 서명했고, 자기 플랫폼용이며, 서명된 크기와 SHA-256이 맞고, 새 바이너리가 실제로 뜨는(`--version`) 경우에만 설치합니다. 내 기기는 기본으로 받고(`--no-auto-upgrade`로 거부), 남의 기기는 주인이 에이전트를 `--auto-upgrade`로 띄운 경우에만 받습니다. 쉬는 노드는 바로, 바쁜 노드는 한 대씩 새 작업을 멈추고 비운 뒤 교체합니다. 에이전트는 제자리에서 재시작하고, 새 바이너리를 클러스터가 거부하거나, 5분 동안 클러스터에 못 붙거나, 계속 죽으면 이전 바이너리로 되돌리고 실패를 `pumasi upgrade status`에 보고합니다. 컨트롤 플레인을 먼저 업그레이드하세요(멈추고 바이너리 교체 후 시작). 프로토콜 버전이 다른 바이너리는 `--force`가 필요하고, 그 에이전트들은 롤백됩니다.

## 명령어

| 명령 | 설명 |
| --- | --- |
| `pumasi cluster start` | 컨트롤 플레인 실행. 처음 실행하면 클러스터를 만들고, 상태는 `~/.pumasi/cluster`에 저장합니다. |
| `pumasi token [--ttl 24h] [--uses N]` | 내 다른 기기용 참가 티켓 발급 |
| `pumasi invite <계정> [--ttl 7d] [--uses 1] [--allow-docker]` | 다른 사람의 기기를 `계정` 몫으로 버는 제공자로 초대 |
| `pumasi agent join <티켓> [--allow-docker] [--allow-microvm]` | 클러스터에 참가하고 작업 실행 시작. `--allow-docker`면 Docker 작업, `--allow-microvm`이면 microVM 작업도 받음 (`--vm-backend firecracker\|qemu`, `--vm-dir DIR`) |
| `pumasi agent start [--allow-docker] [--allow-microvm] [--auto-upgrade\|--no-auto-upgrade]` | 이전에 참가한 클러스터에 티켓 없이 재접속 |
| `pumasi service install [--ticket T] [--user\|--system] [--dry-run] [-- 에이전트 옵션]` | 부팅 시 시작하는 서비스로 에이전트 설치 (systemd, launchd, Windows 서비스). `status`, `uninstall`도 있음 |
| `pumasi upgrade rollout 파일... [--wait] [--force]` | 클러스터 키로 서명한 새 에이전트 바이너리를 노드에 배포. `upgrade status`로 노드별 상태, `upgrade cancel`로 중단 |
| `pumasi vm setup [--guest FILE] [--force]` | 게스트 커널, Firecracker(Linux) 또는 QEMU(Windows, 관리자), 게스트 init을 `~/.pumasi/vm`에 설치 |
| `pumasi vm status` | microVM 준비 상태와 사용할 VM 모니터 표시 |
| `pumasi vm run 이미지 [--input DIR] [-e K=V] [--memory-mb N] [--cpus N] [--timeout T] [-- 명령...]` | 에이전트와 똑같이 이 기기의 microVM에서 이미지 실행 (`/output`은 `pumasi-out/vm`에 저장) |
| `pumasi run job.toml [--account A] [--output] [--out DIR]` | 작업 제출 후 완료까지 대기. 결과 파일은 `DIR`(기본 `pumasi-out/<작업 ID>`)에 저장. 성공 시 종료 코드 0, 실패 태스크가 있으면 2. `--account`는 제공자 실행 비용을 낼 계정(기본 `owner`) |
| `pumasi status` | 노드(역할, 온라인 여부, 실행기, 슬롯, 실행 중, 검증 기록)와 최근 작업 및 비용 |
| `pumasi job <ID> [--output] [--out DIR]` | 태스크별 상태, 노드, 시도 횟수, 시간, 비용, 검증 결과, 출력. `--out`이면 결과 파일 다운로드 |
| `pumasi credit list \| add <계정> <금액> \| payout <계정> <금액> \| ledger` | 잔액(사용 가능, 보류, 지출, 수익), 선불 충전, 지급 기록, 원장 |
| `pumasi prices [set ...]` | 요금표(`--microvm-cpu-hour` 포함)와 정산 정책(보류 시간, 검증 비율, 최대 스트라이크) 조회·변경 |
| `pumasi receipts <ID> [--export FILE]` | 작업의 서명된 사용 영수증 |
| `pumasi verify-receipts FILE [--cluster ID]` | 내보낸 영수증을 오프라인으로 검증 |
| `pumasi cancel <ID>` | 작업 취소 |
| `pumasi remove-node <ID>` | 노드 퇴출(다시 오려면 새 티켓 필요) |
| `pumasi login <관리자 티켓>` | 다른 기기에서 원격 클러스터 관리 |
| `pumasi exec module.wasm [--input DIR] [--seed S] [인자...]` | 에이전트와 같은 샌드박스로 로컬 실행 (`DIR`이 `/input`으로 보이고 `/output`은 `pumasi-out/exec`에 저장, `--seed`는 결정적 모드) |

`--home DIR` 또는 `PUMASI_HOME`으로 상태 디렉터리를 바꾸면 한 기기에서 클러스터와 에이전트 여러 개를 띄워 시험할 수 있습니다.

## 작업 명세 (job.toml)

작업은 세 실행기 중 하나에서 돌고, 명세 형식과 명령은 같습니다.

| | `wasm` (기본) | `docker` | `microvm` |
| --- | --- | --- | --- |
| 실행 대상 | `wasm32-wasip1` 모듈 | Linux OCI 이미지 | 공개 Linux OCI 이미지 |
| 실행 위치 | 모든 노드 | `--allow-docker`로 시작한 노드만 (제공자는 초대에도 `--allow-docker` 필요) | `--allow-microvm`으로 시작한 노드 (제공자 포함) |
| 격리 | Wasmtime 샌드박스, 모르는 코드도 안전 | 컨테이너, 내 기기나 믿는 사람끼리만 | 태스크마다 자기 커널을 가진 VM |
| 네트워크 | 불가 | 기본 차단, `network = true`로 허용 | 불가 |
| GPU | 불가 | `gpus = "all"` | 아직 불가 |
| 파일 | `/input`, `/output` | `/input`, `/output` | `/input`, `/output` |

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

microVM 작업 (같은 이미지와 명령을 VM에서):

```toml
name = "wordcount-vm"
executor = "microvm"                  # 필수: image만 쓰면 docker
image = "python:3.12-slim"
command = ["python", "/input/count.py", "{shard}", "{shards}"]
files = ["count.py", "texts/"]
shards = 4

[resources]
memory_mb = 256                       # VM은 여기에 커널용 약 96 MiB를 더 받음
cpus = 1                              # 정수 vCPU로 올림
timeout = "2m"                        # 부팅과 실행 포함, 첫 이미지 받기는 제외
```

`files`의 경로는 job.toml 기준이고, 폴더는 하위까지 그대로 `/input` 아래에 복사됩니다. 한 작업의 입력 파일은 합쳐서 48 MiB까지입니다. 태스크가 `/output`에 쓴 파일(태스크당 32 MiB까지)은 컨트롤 플레인에 저장되고, `pumasi run`이 `pumasi-out/<작업 ID>/task-<N>/`로 내려받습니다. stdout·stderr는 각각 4 MiB까지 보관합니다.

결정적 모드(WASM 작업 기본값)에서는 시계가 2000-01-01에서 시작해 읽을 때마다 1ms씩 가고, 난수는 작업과 태스크 번호로 만든 시드에서 나옵니다. 실제 시간이나 새 난수가 필요한 작업은 `deterministic = false`로 두고, 그러면 제공자 실행은 검증하지 않습니다.

`shards` 대신 `[[task]]`로 태스크를 하나씩 나열할 수도 있고, 태스크마다 `files = [...]`로 그 태스크만 받는 파일을 줄 수 있습니다.

컨테이너는 `network = true`가 아니면 `--network none`으로, 메모리·CPU·프로세스 수 상한과 `no-new-privileges`를 걸고, 호스트 폴더를 마운트하지 않은 채(파일은 복사로 주고받음) 실행됩니다. 이미지는 처음 쓸 때 받아 옵니다.

microVM 태스크는 `docker run`처럼 이미지의 entrypoint와 명령(또는 작업의 `command`)을 이미지에 지정된 사용자로, `/input`을 작업 디렉터리로 해서 실행합니다. 루트 파일시스템은 모든 태스크가 읽기 전용으로 공유하는 이미지 위에 태스크마다 새 쓰기 층을 얹은 것이고, 끝나면 버립니다. KVM이 있으면 태스크 하나가 1초 안쪽으로 부팅됩니다.

## 보안 모델

- 참가 티켓은 만료 시간과 사용 횟수를 가진 비밀값입니다. 한 번 참가한 노드는 공개키로 기억되어 다시 티켓이 필요 없고, `remove-node`로 퇴출합니다.
- `~/.pumasi/cluster/admin.ticket`은 전체 권한이므로 비밀번호처럼 다뤄야 합니다.
- WASM 태스크는 실행 기기로부터 격리(자기 `/input`·`/output`만 보임, 네트워크 차단, 메모리·CPU 상한)됩니다.
- Docker 태스크는 컨테이너 수준의 격리라 호스트 커널을 공유합니다. 그래서 기기 주인이 `--allow-docker`를 줘야만 실행합니다. 내 기기나 믿을 수 있는 사람끼리의 클러스터에서 씁니다.
- microVM 태스크는 작업으로부터 제공자의 기기를 보호합니다. 태스크마다 KVM 위에서 자기 커널을 쓰고(Firecracker는 seccomp로 자신도 가둠), 네트워크 장치가 없고, 자기 디스크만 보이며, 메모리와 vCPU 상한이 걸립니다. 이미지 레이어도 호스트가 아니라 VM 안에서 풉니다. 다만 제공자는 태스크가 하는 일을 볼 수 있습니다(오픈 등급).
- 제공자 기기(`pumasi invite`로 받은 남의 기기)는 WASM과 microVM 작업을 받고, Docker는 초대가 허용할 때만 받으며, 검증의 판정은 내 기기만 맡습니다. microVM 결과는 결정적이지 않아 표본 검증 대상이 아닙니다.
- 제공자는 가짜 결과를 낼 수 있습니다. 서명은 누가 무엇을 주장했는지를 증명할 뿐 참임을 증명하지는 않습니다. 표본 검증이 확률적으로 잡습니다. 기본 10%면 가짜 결과 20개를 낸 제공자는 88% 확률로 걸리고, 걸린 수익은 몰수되며, 3번이면 정지됩니다. 수익을 1시간 보류하는 이유가 이것입니다.
- 어느 쪽이든 기기 주인에게 내용이 숨겨지지는 않습니다. 지금은 "오픈" 등급만 있습니다.
- 에이전트 업그레이드는 에이전트가 참가할 때 알게 된 컨트롤 플레인 키로 서명되고, 실행 전에 서명된 SHA-256과 대조합니다. 관리자 티켓을 가진 사람은 내 기기(와 옵트인한 제공자 기기)에 바이너리를 배포할 수 있으므로, 관리자 티켓은 그 기기들의 root 권한만큼 중요합니다. 릴리스 바이너리에는 `SHA256SUMS`가 있고, 저장소가 공개되면 GitHub 빌드 출처 증명도 붙습니다(`gh attestation verify pumasi-linux-x86_64 -R syang0531/pumasi`).

## 로드맵

| 단계 | 범위 | 상태 |
| --- | --- | --- |
| 1 | 사설 클러스터: 에이전트, 컨트롤 플레인, iroh 연결, WASM·Docker 실행기, 파일 입출력, CLI | 완료 |
| 2 | 초대한 외부 제공자(오픈 등급), 노드·클러스터 공동 서명 사용 영수증, 고정 요금표, 선불 크레딧 원장, 표본 재실행 검증 | 완료 |
| 3 | microVM 실행기: Docker 작업과 같은 OCI 이미지를 Firecracker·QEMU VM에서, 같은 작업 명세로, 제공자에게도 | 완료 |
| 4 | 배포·설치: 6개 플랫폼 릴리스 바이너리와 SHA256SUMS, 설치 스크립트, OS 서비스, 클러스터 서명 업그레이드와 롤백 | 완료 |
| 5 | Windows 정식 지원: Docker Desktop(WSL 2), QEMU + WHPX microVM(없으면 TCG), 고정 버전 QEMU 설치, Job Object, 내장 ext4 포맷터와 게스트 init | 이번 릴리스 |
| 6 | 구형 Intel Mac(macOS 10.15+), Virtualization.framework microVM, 가용 정책(전원 연결 시, 유휴 시, 시간대) | 예정 |
| 7 | 성능 벤치마크와 컴퓨팅 유닛(CU): 노드가 서명한 성능 프로필(CPU, 메모리 대역폭, fio로 디스크 종류·속도, SMART 마모도, 지연), 1 CU = 기준 vCPU 1개를 1시간 쓰는 값과 자원별 CU 단가, 정수 milli-CU 원장, 돈 없이 서로 빌려 쓰는 그룹 로컬 원장 | 예정 |
| 8 | 클러스터 파일 저장소(iroh-blobs): 데이터셋, 대용량 입출력, 이어받기, 저장 용량 CU 과금, 비공개 레지스트리 이미지 | 예정 |
| 9 | GPU·microVM 장치: 탐지, GPU 벤치마크와 CU 환산, 스케줄링, Docker GPU 작업, VFIO 패스스루, microVM 네트워크 | 예정 |
| 10 | 클러스터 관리: 라벨·selector, drain, 우선순위, 쿼터, 역할 티켓, `apply` | 예정 |
| 11 | 모니터링·경보: 노드 지표, Prometheus, 실시간 로그, 웹훅 | 예정 |
| 12 | 웹 대시보드(바이너리 내장) | 예정 |
| 13 | 컨트롤 플레인 고가용성(Raft) | 예정 |
| 14 | WebGPU 실행기(실험) | 예정 |
| 15 | SEV-SNP·TDX 서버 기반 기밀 등급과 원격 증명 | 예정 |
| 16 | 허브(별도 비공개 서비스): 제공자 발견, 지역 지연 측정, 신뢰도 점수, 수수료가 있는 그룹 간 CU 원장 | 예정 |
| 17 | 허브에서 원화로 CU 구매(토스페이먼츠). 환전과 사용자 간 양도 없음 | 예정 |

알려진 한계: microVM은 공개 이미지만 받고, 네트워크와 GPU가 아직 없으며, 빠르게 돌리려면 KVM(Linux)이나 WHPX(Windows x86_64)가 필요합니다(없으면 QEMU가 동작은 하지만 느리고, macOS의 QEMU 경로는 아직 검증하지 않았으며, ARM Windows는 아직 microVM이 없습니다). CI 러너의 WHPX는 중첩 환경이라 QEMU 디스크가 깨지고 에뮬레이션으로 넘어가므로, WHPX는 실기에서만 확인합니다.

## 라이선스

Apache-2.0
