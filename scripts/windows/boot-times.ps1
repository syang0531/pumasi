# Boot times of a Linux VM on this PC (phase 27, 1.11): Pumasi's microVMs
# (QEMU with WHPX) against a VM that Hyper-V's compute service (HCS)
# starts, measured through WSL 2, whose VM is one. An HCS backend for
# microVMs would be adopted only if it were clearly faster
# (docs/architecture.md, "Phase 27: Windows").
#
# Run in an administrator PowerShell after `pumasi vm setup` and
# `pumasi docker setup` (it borrows the Ubuntu Base image that downloads):
#
#   powershell -ExecutionPolicy Bypass -File scripts\windows\boot-times.ps1
#
# It imports a throwaway distro for you (pumasi-boot-probe), runs
# `wsl --shutdown` between runs (which stops your own WSL distros, Docker
# Desktop's too; the pumasi-docker distro belongs to another account and
# keeps running), and removes the throwaway distro at the end.
param(
    [string]$Pumasi = "pumasi",
    [int]$Runs = 5
)
$ErrorActionPreference = "Stop"

$rootfs = Get-ChildItem "$env:ProgramData\Pumasi\wsl-docker\downloads\ubuntu-base-*.tar.gz" -ErrorAction SilentlyContinue |
    Select-Object -First 1
if (-not $rootfs) { throw "run 'pumasi docker setup' first: this uses the Ubuntu Base image it downloads" }

$probe = "pumasi-boot-probe"
$dir = Join-Path $env:TEMP $probe
wsl.exe --unregister $probe 2>$null | Out-Null
wsl.exe --import $probe $dir $rootfs.FullName --version 2 | Out-Null

function Median($xs) {
    $s = @($xs | Sort-Object)
    $s[[int][math]::Floor(($s.Count - 1) / 2)]
}
function Show($label, $xs) {
    "{0}: median {1:N0} ms (runs: {2})" -f $label, (Median $xs), (($xs | ForEach-Object { "{0:N0}" -f $_ }) -join ", ")
}

try {
    # Warm-up: the image is pulled and unpacked once, outside the timings.
    & $Pumasi vm run busybox:1.36 -- true *> $null
    & $Pumasi vm status | Select-String -Pattern "whpx|accelerat|backend" | ForEach-Object { "vm status: $_" }

    $wsl = @(); $qemu = @()
    for ($i = 1; $i -le $Runs; $i++) {
        wsl.exe --shutdown
        Start-Sleep -Seconds 3
        $wsl += (Measure-Command { wsl.exe -d $probe --exec /bin/true }).TotalMilliseconds
        $qemu += (Measure-Command { & $Pumasi vm run busybox:1.36 -- true *> $null }).TotalMilliseconds
    }
    ""
    Show "WSL 2 cold start (an HCS VM boots Linux, then the distro starts)" $wsl
    Show "Pumasi microVM, QEMU + WHPX (disks, boot, run true, exit)     " $qemu
    ""
    "Send both lines (and the 'vm status' lines) back. HCS is worth a backend only if its time is well under half of QEMU's."
}
finally {
    wsl.exe --unregister $probe 2>$null | Out-Null
}
