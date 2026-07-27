param(
  [switch]$Recreate
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

# If a container named trader-kafka was created manually before, Compose cannot
# create another container with the same name. Use -Recreate to replace it.
$existingContainerId = docker ps -a --filter "name=^/trader-kafka$" --format "{{.ID}}"
if ($existingContainerId -and $Recreate) {
  Write-Host "Removing existing trader-kafka container..."
  docker rm -f trader-kafka | Out-Host
}

if ($existingContainerId -and -not $Recreate) {
  Write-Host "A container named trader-kafka already exists."
  Write-Host "If Compose reports a name conflict, run:"
  Write-Host "  .\scripts\run-local-kafka.cmd -Recreate"
  Write-Host ""
}

# Start only the local Kafka service using values from .env.db.
docker compose `
  --env-file ".env.db" `
  -f "docker-compose.local-db.yml" `
  up -d kafka

# Wait until the Apache Kafka healthcheck passes.
for ($i = 0; $i -lt 30; $i++) {
  $status = docker inspect trader-kafka --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}" 2>$null
  if ($status -eq "healthy") {
    break
  }

  Start-Sleep -Seconds 2
}

$finalStatus = docker inspect trader-kafka --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}" 2>$null
if ($finalStatus -ne "healthy") {
  docker logs trader-kafka --tail 80
  throw "Kafka did not become healthy. Current status: $finalStatus"
}

# Create the control-plane topic and source-specific raw-ready topics.
# Job events stay small and latency-sensitive, while raw-ready events are split
# by source so lag thresholds and ETL workers can be operated independently.
$topics = @(
  @{ Name = "trader.jobs.events"; Partitions = 3 },
  @{ Name = "trader.raw.kis.ready"; Partitions = 3 },
  @{ Name = "trader.raw.bls.ready"; Partitions = 1 },
  @{ Name = "trader.raw.sec.ready"; Partitions = 3 },
  @{ Name = "trader.raw.news.ready"; Partitions = 3 }
)

foreach ($topic in $topics) {
  docker exec trader-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --if-not-exists `
    --topic $topic.Name `
    --partitions $topic.Partitions `
    --replication-factor 1
}

foreach ($topic in $topics) {
  docker exec trader-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic $topic.Name
}

Write-Host ""
Write-Host "Kafka is ready at localhost:9092"
Write-Host "Topics are ready:"
foreach ($topic in $topics) {
  Write-Host "  - $($topic.Name)"
}
