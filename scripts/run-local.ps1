$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Missing $envPath. Copy .env.example to .env and set local values first."
}

# Load KEY=VALUE entries into this process without printing secret values.
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

# The data backfill and Controller use the same local PostgreSQL database.
# When the Controller still has the copied example URL, reuse the verified URL
# from trader-data/.env instead of asking for duplicate credentials.
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

foreach ($required in @("DATABASE_URL", "ADMIN_API_KEY")) {
    if (-not [Environment]::GetEnvironmentVariable($required, "Process")) {
        throw "$required is missing from $envPath"
    }
}

Set-Location -LiteralPath $projectRoot
go run -buildvcs=false ./cmd/controller
