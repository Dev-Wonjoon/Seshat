# 기존 로그 파일을 읽는 Seshat 에이전트를 현재 터미널에서 실행한다.
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$LogPath,

    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$RunId,

    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$SourceId,

    [ValidateSet('CENTRAL', 'AGENT')]
    [string]$Mode = 'CENTRAL',

    [uri]$Endpoint = 'http://127.0.0.1:18080/api/logs/batches',

    [ValidateRange(1, [int]::MaxValue)]
    [int]$MaxPendingBatches = 128,

    [string]$QueuePath
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($RunId) -or [string]::IsNullOrWhiteSpace($SourceId)) {
    throw '실행 ID와 소스 ID를 입력하세요.'
}
if (-not $Endpoint.IsAbsoluteUri -or $Endpoint.Scheme -notin @('http', 'https') -or -not $Endpoint.Host) {
    throw '중앙 주소에는 HTTP 또는 HTTPS 절대 URL을 입력하세요.'
}

$logFile = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($LogPath)
if ([string]::IsNullOrWhiteSpace($QueuePath)) {
    # 실행 ID에 경로 문자가 포함되어도 같은 실행과 소스는 같은 큐를 사용한다.
    $identity = ConvertTo-Json -InputObject @($RunId, $SourceId) -Compress
    $digest = [System.Security.Cryptography.SHA256]::Create()
    try {
        $key = [System.BitConverter]::ToString(
            $digest.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($identity))
        ).Replace('-', '').ToLowerInvariant()
    } finally {
        $digest.Dispose()
    }
    $queueDirectory = Join-Path (Join-Path $env:LOCALAPPDATA 'Seshat\agent-queues') $key
} else {
    $queueDirectory = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($QueuePath)
}

$checkpoint = Join-Path $queueDirectory 'checkpoint.properties'
if (-not (Test-Path -LiteralPath $logFile -PathType Leaf) -and
    -not (Test-Path -LiteralPath $checkpoint -PathType Leaf)) {
    throw "로그 파일을 찾을 수 없습니다. 빈 파일이나 작성기가 만든 로그를 준비하세요. $logFile"
}

$java = Get-Command java.exe -CommandType Application -ErrorAction SilentlyContinue |
    Select-Object -First 1
if (-not $java) {
    throw 'Java 25를 설치하고 PATH에 추가하세요.'
}

$jar = Join-Path $PSScriptRoot 'build\libs\agent.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) {
    Write-Host '에이전트 JAR을 빌드합니다.'
    Push-Location -LiteralPath $PSScriptRoot
    try {
        & .\gradlew.bat agentJar
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) {
            throw '에이전트 JAR 빌드에 실패했습니다.'
        }
    } finally {
        Pop-Location
    }
}

Write-Host "에이전트 실행 $RunId / $SourceId ($Mode)"
Write-Host "로그 파일 $logFile"
Write-Host "큐 디렉터리 $queueDirectory"
Write-Host 'Ctrl+C로 종료할 수 있습니다.'

$agentArgs = @(
    '-jar'
    $jar
    $RunId
    $SourceId
    $Mode.ToUpperInvariant()
    $logFile
    $Endpoint.AbsoluteUri
    $MaxPendingBatches.ToString()
    $queueDirectory
)
& $java.Source @agentArgs
if ($LASTEXITCODE -ne 0) {
    throw "에이전트가 종료 코드 $LASTEXITCODE 로 종료됐습니다."
}
