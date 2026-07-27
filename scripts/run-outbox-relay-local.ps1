$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Missing $envPath. Copy .env.example to .env and set local values first."
}

# .env의 KEY=VALUE를 현재 PowerShell 프로세스 환경 변수로 올린다.
# 비밀번호나 API key를 콘솔에 출력하지 않기 위해 값은 따로 찍지 않는다.
foreach ($line in Get-Content -LiteralPath $envPath) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith("#")) {
        continue
    }

    $separator = $trimmed.IndexOf("=")
    if ($separator -le 0) {
        continue
    }

    $name = $trimmed.Substring(0, $separator).Trim()
    $value = $trimmed.Substring($separator + 1).Trim().Trim('"', "'")
    [Environment]::SetEnvironmentVariable($name, $value, "Process")
}

# Controller 예제 DB 주소를 그대로 둔 경우 trader-data/.env의 실제 DATABASE_URL을 재사용한다.
# 로컬 백필과 outbox relay가 같은 DB를 보게 해서 환경 변수 중복 관리를 줄인다.
$exampleDatabaseUrl = "postgres://trader:trader@localhost:5432/trader?sslmode=disable"
$databaseUrl = [Environment]::GetEnvironmentVariable("DATABASE_URL", "Process")
$dataEnvPath = Join-Path (Split-Path -Parent $projectRoot) "trader-data\.env"
if (($databaseUrl -eq $exampleDatabaseUrl -or -not $databaseUrl) -and
    (Test-Path -LiteralPath $dataEnvPath)) {
    foreach ($line in Get-Content -LiteralPath $dataEnvPath) {
        if ($line -match '^\s*DATABASE_URL\s*=\s*(.+?)\s*$') {
            $value = $matches[1].Trim().Trim('"', "'")
            [Environment]::SetEnvironmentVariable("DATABASE_URL", $value, "Process")
            break
        }
    }
}

foreach ($required in @("DATABASE_URL", "KAFKA_BROKERS")) {
    if (-not [Environment]::GetEnvironmentVariable($required, "Process")) {
        throw "$required is missing from $envPath"
    }
}

Set-Location -LiteralPath $projectRoot
go run -buildvcs=false ./cmd/outbox-relay
