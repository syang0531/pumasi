# Installs pumasi.exe on Windows from a GitHub release, checks its SHA-256
# against the release's SHA256SUMS, adds it to PATH, and optionally
# installs the agent as a Windows service that joins a cluster.
#
#   irm https://github.com/syang0531/pumasi/releases/latest/download/install.ps1 | iex
#
# With options (from an administrator PowerShell for -Ticket):
#
#   & ([scriptblock]::Create((irm https://github.com/syang0531/pumasi/releases/latest/download/install.ps1))) `
#       -Ticket pumasijoin... -AgentArgs '--allow-docker'
#
# Parameters can also come from the environment: PUMASI_VERSION,
# PUMASI_TICKET, PUMASI_BASE_URL, PUMASI_REPO, GITHUB_TOKEN (private
# repository).
param(
    # Release tag to install (default: latest).
    [string]$Version = $(if ($env:PUMASI_VERSION) { $env:PUMASI_VERSION } else { 'latest' }),
    # Install the agent service and join the cluster with this ticket.
    [string]$Ticket = $env:PUMASI_TICKET,
    # Where to put pumasi.exe (default: Program Files\Pumasi as
    # administrator, else %LOCALAPPDATA%\Programs\Pumasi).
    [string]$Dir,
    # Download from this URL or local directory instead of GitHub.
    [string]$BaseUrl = $env:PUMASI_BASE_URL,
    [string]$Repo = $(if ($env:PUMASI_REPO) { $env:PUMASI_REPO } else { 'syang0531/pumasi' }),
    # Agent options for the service, e.g. '--allow-docker'.
    [string[]]$AgentArgs = @(),
    # Do not add the install directory to PATH.
    [switch]$NoPath
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

function Get-Arch {
    try {
        $a = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    } catch {
        $a = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    }
    switch -Regex ($a) {
        '^(X64|AMD64)$' { 'x86_64' }
        '^(Arm64|ARM64)$' { 'aarch64' }
        default { throw "unsupported CPU architecture $a" }
    }
}

$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
$asset = "pumasi-windows-$(Get-Arch).exe"
if (-not $Dir) {
    $Dir = if ($admin) { Join-Path $env:ProgramFiles 'Pumasi' } else { Join-Path $env:LOCALAPPDATA 'Programs\Pumasi' }
}
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("pumasi-install-" + [Guid]::NewGuid())
New-Item -ItemType Directory -Path $tmp | Out-Null

$release = $null
function Get-AssetUrl([string]$name) {
    if (-not $script:release) {
        $path = if ($Version -eq 'latest') { 'releases/latest' } else { "releases/tags/$Version" }
        $script:release = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$Repo/$path" `
            -Headers @{ Authorization = "Bearer $env:GITHUB_TOKEN" }
    }
    $a = $script:release.assets | Where-Object { $_.name -eq $name } | Select-Object -First 1
    if (-not $a) { throw "release $Version of $Repo has no asset $name" }
    $a.url
}

function Get-Asset([string]$name, [string]$out) {
    if ($BaseUrl) {
        if ($BaseUrl -match '://') {
            Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/$name" -OutFile $out
        } else {
            Copy-Item (Join-Path $BaseUrl $name) $out
        }
    } elseif ($env:GITHUB_TOKEN) {
        Invoke-WebRequest -UseBasicParsing -Uri (Get-AssetUrl $name) -OutFile $out `
            -Headers @{ Authorization = "Bearer $env:GITHUB_TOKEN"; Accept = 'application/octet-stream' }
    } else {
        $base = if ($Version -eq 'latest') { "https://github.com/$Repo/releases/latest/download" }
                else { "https://github.com/$Repo/releases/download/$Version" }
        try {
            Invoke-WebRequest -UseBasicParsing -Uri "$base/$name" -OutFile $out
        } catch {
            throw "could not download $name (for a private repository set GITHUB_TOKEN): $_"
        }
    }
}

try {
    Write-Host "downloading $asset ($Version)"
    $sums = Join-Path $tmp 'SHA256SUMS'
    $exeTmp = Join-Path $tmp 'pumasi.exe'
    Get-Asset 'SHA256SUMS' $sums
    Get-Asset $asset $exeTmp

    $want = $null
    foreach ($line in Get-Content $sums) {
        $parts = $line -split '\s+', 2
        if ($parts.Count -eq 2 -and $parts[1].TrimStart('*') -eq $asset) { $want = $parts[0].ToLower() }
    }
    if (-not $want) { throw "SHA256SUMS has no entry for $asset" }
    $got = (Get-FileHash -Algorithm SHA256 $exeTmp).Hash.ToLower()
    if ($got -ne $want) { throw "checksum mismatch for ${asset}: got $got, expected $want" }
    Write-Host "checksum ok ($got)"

    # Try it before it replaces anything: Smart App Control and other
    # application control policies block unsigned binaries at this point.
    $newVersion = $null
    try { $newVersion = & $exeTmp --version 2>&1 } catch { $newVersion = $_.Exception.Message }
    if ($LASTEXITCODE -ne 0 -or -not ("$newVersion" -match '^pumasi ')) {
        throw ("Windows did not run the downloaded pumasi.exe: $newVersion`n" +
            'If Smart App Control is on (Windows Security > App & browser control), it blocks ' +
            'unsigned programs, and Pumasi''s binaries are not code-signed yet. See ' +
            '"Windows Smart App Control" in https://github.com/syang0531/pumasi#readme')
    }

    New-Item -ItemType Directory -Force -Path $Dir | Out-Null
    $exe = Join-Path $Dir 'pumasi.exe'
    if (Test-Path $exe) {
        # A running pumasi.exe cannot be overwritten, but can be renamed.
        $old = "$exe.old"
        Remove-Item -Force $old -ErrorAction SilentlyContinue
        Move-Item -Force $exe $old
    }
    Move-Item -Force $exeTmp $exe
    Write-Host "installed $exe ($(& $exe --version))"

    if (-not $NoPath) {
        $scope = if ($admin) { 'Machine' } else { 'User' }
        $path = [Environment]::GetEnvironmentVariable('Path', $scope)
        if (-not (($path -split ';') -contains $Dir)) {
            [Environment]::SetEnvironmentVariable('Path', ($path.TrimEnd(';') + ";$Dir"), $scope)
            $env:Path = "$env:Path;$Dir"
            Write-Host "added $Dir to the $scope PATH (open a new terminal to use it)"
        }
    }

    # Services keep running the old binary (renamed aside) until restarted.
    if ($admin) {
        foreach ($name in 'pumasi-cluster', 'pumasi-agent', 'pumasi-ui') {
            $svc = Get-Service -Name $name -ErrorAction SilentlyContinue
            if ($svc -and $svc.Status -eq 'Running') {
                Write-Host "restarting the $name service"
                Restart-Service -Name $name
            }
        }
    }

    if ($Ticket) {
        if (-not $admin) { throw 'installing the service needs an administrator PowerShell' }
        Write-Host 'installing the agent service'
        & $exe service install --ticket $Ticket '--' @AgentArgs
        if ($LASTEXITCODE -ne 0) { throw "service install failed ($LASTEXITCODE)" }
    }
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
