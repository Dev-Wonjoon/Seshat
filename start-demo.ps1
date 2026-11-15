# 시연용 중앙 서버를 현재 터미널에서 실행한다.
$ErrorActionPreference = 'Stop'

Push-Location -LiteralPath $PSScriptRoot
try {
    $port = 18080
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
    try {
        $listener.Start()
    } catch [System.Net.Sockets.SocketException] {
        throw "포트 $port 를 사용할 수 없습니다. 해당 포트에서 실행 중인 서버를 먼저 종료하세요."
    } finally {
        $listener.Stop()
    }

    $docker = Get-Command docker.exe -ErrorAction SilentlyContinue
    $java = Get-Command java.exe -ErrorAction SilentlyContinue
    if (-not $docker -or -not $java) {
        throw 'Docker Desktop과 Java 25를 설치하고 PATH에서 사용할 수 있게 설정하세요.'
    }

    Write-Host 'Elasticsearch를 시작합니다.'
    & $docker.Source compose up -d elasticsearch
    if ($LASTEXITCODE -ne 0) {
        throw 'Elasticsearch를 시작하지 못했습니다. Docker Desktop 실행 상태를 확인하세요.'
    }

    $es = 'http://127.0.0.1:9200'
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($true) {
        try {
            $null = Invoke-WebRequest -Uri "$es/" -UseBasicParsing -TimeoutSec 3
            break
        } catch {
            if ($timer.Elapsed.TotalSeconds -ge 90) {
                throw '90초 안에 Elasticsearch가 응답하지 않았습니다. docker compose logs elasticsearch로 확인하세요.'
            }
            Start-Sleep -Seconds 1
        }
    }

    $index = "$es/seshat-logs-v1"
    try {
        $null = Invoke-WebRequest -Uri $index -Method Head -UseBasicParsing -TimeoutSec 5
    } catch {
        if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 404) {
            throw
        }
        $mapping = Join-Path $PSScriptRoot 'src/main/resources/elasticsearch/seshat-logs-v1.json'
        $indexArgs = @{
            Uri = $index
            Method = 'Put'
            InFile = $mapping
            ContentType = 'application/json'
            UseBasicParsing = $true
            TimeoutSec = 15
        }
        $null = Invoke-WebRequest @indexArgs
    }

    Write-Host '중앙 서버와 에이전트를 빌드합니다.'
    & .\gradlew.bat bootJar agentJar
    if ($LASTEXITCODE -ne 0) {
        throw '중앙 서버 또는 에이전트 빌드에 실패했습니다.'
    }

    Write-Host "시연용 중앙 서버를 http://127.0.0.1:$port 에서 실행합니다."
    Write-Host 'Ctrl+C로 종료할 수 있습니다.'
    $serverArgs = @(
        '-jar'
        'build/libs/seshat-0.0.1-SNAPSHOT.jar'
        '--server.address=127.0.0.1'
        "--server.port=$port"
        '--spring.elasticsearch.uris=http://127.0.0.1:9200'
        '--spring.datasource.url=jdbc:sqlite:./demo-m7.db?journal_mode=WAL&synchronous=FULL&busy_timeout=5000'
        '--seshat.queue.worker-enabled=true'
        '--seshat.control.auto.enabled=false'
        '--seshat.repair.enabled=false'
        '--seshat.demo.enabled=true'
        '--seshat.demo.auto.enabled=true'
    )
    & $java.Source @serverArgs
    if ($LASTEXITCODE -ne 0) {
        throw "중앙 서버가 종료 코드 $LASTEXITCODE 로 종료됐습니다."
    }
} finally {
    Pop-Location
}
