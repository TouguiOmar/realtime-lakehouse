# ─────────────────────────────────────────────────────────────────
#  start.ps1  —  Real-Time Lakehouse full startup script (Windows)
#  Equivalent to: make demo + launches bronze in Terminal 2
#  Run from the project root: .\start.ps1
# ─────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Load .env ─────────────────────────────────────────────────────
$envFile = Join-Path $ProjectRoot ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "ERROR: .env file not found. Copy .env.example to .env first." -ForegroundColor Red
    exit 1
}
Get-Content $envFile | Where-Object { $_ -match "^\s*[^#].*=.*" } | ForEach-Object {
    $parts = $_ -split "=", 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
}

# ── Helpers ───────────────────────────────────────────────────────
function Write-Step($msg)    { Write-Host "`n>> $msg" -ForegroundColor Cyan }
function Write-OK($msg)      { Write-Host "   OK  $msg" -ForegroundColor Green }
function Write-Warn($msg)    { Write-Host "   WARN $msg" -ForegroundColor Yellow }
function Write-Fail($msg)    { Write-Host "   ERR  $msg" -ForegroundColor Red; exit 1 }

$PACKAGES_NO_KAFKA = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0," +
                     "org.apache.hadoop:hadoop-aws:3.3.4," +
                     "com.amazonaws:aws-java-sdk-bundle:1.12.262"

$PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0," +
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," +
            "org.apache.hadoop:hadoop-aws:3.3.4," +
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"

# ── Step 1: Start stack ───────────────────────────────────────────
Write-Step "Starting Docker stack"
Set-Location $ProjectRoot
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Fail "docker compose up failed" }
Write-OK "Stack started"

# ── Step 2: Wait for healthy ──────────────────────────────────────
Write-Step "Waiting for services to be healthy"
$maxWait = 150; $elapsed = 0; $interval = 10
while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds $interval
    $elapsed += $interval
    $all     = docker compose ps --format "{{.Status}}"
    $healthy = ($all | Where-Object { $_ -match "healthy" }).Count
    $total   = ($all | Where-Object { $_ -match "Up" }).Count
    Write-Warn "$healthy/$total healthy ($elapsed s)"
    if ($healthy -ge 8) { Write-OK "All services healthy"; break }
}

# ── Step 3: Register Debezium ─────────────────────────────────────
Write-Step "Registering Debezium connector"
Start-Sleep -Seconds 5
$status = docker exec lakehouse-connect curl -s -o /dev/null -w "%{http_code}" `
    -X GET http://localhost:8083/connectors/orders-cdc/status
if ($status -eq "200") {
    Write-OK "Connector already running"
} else {
    docker exec lakehouse-connect curl -s -X POST http://localhost:8083/connectors `
        -H "Content-Type: application/json" `
        -d "@/debezium/register-connector.json" | Out-Null
    Write-OK "Connector registered"
}

# ── Step 4: Seed data ─────────────────────────────────────────────
Write-Step "Seeding data"
docker exec lakehouse-postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "
INSERT INTO orders (customer_id, status, total_usd) VALUES
  (1, 'pending',   99.99),
  (2, 'completed', 149.50),
  (3, 'pending',   49.00),
  (4, 'completed', 200.00),
  (5, 'pending',   75.00)
ON CONFLICT DO NOTHING;" | Out-Null
Write-OK "Data seeded"

# ── Step 5: Launch Bronze writer in Terminal 2 ────────────────────
Write-Step "Launching Bronze writer in Terminal 2"
$bronzeCmd = "docker exec lakehouse-spark-master /opt/spark/bin/spark-submit " +
    "--master spark://spark-master:7077 --conf spark.cores.max=2 " +
    "--packages $PACKAGES /opt/spark-apps/bronze_writer.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $bronzeCmd
Write-OK "Bronze writer launched in Terminal 2"

# ── Summary ───────────────────────────────────────────────────────
Write-Host ""
Write-Host "___________________________________________"
Write-Host "  Spark Master  →  http://localhost:8080"
Write-Host "  MinIO Console →  http://localhost:9001"
Write-Host "  Airflow       →  http://localhost:8085"
Write-Host "  Kafka Connect →  http://localhost:8083/connectors"
Write-Host "___________________________________________"
Write-Host ""
Write-Host "  Run pipeline:" -ForegroundColor Yellow
Write-Host "    .\run_silver.ps1"
Write-Host "    .\run_gold.ps1"
Write-Host ""