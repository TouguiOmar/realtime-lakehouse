# ─────────────────────────────────────────────
#  run_silver.ps1  —  Run Silver MERGE writer
# ─────────────────────────────────────────────
Write-Host "`n>> Running Silver writer..." -ForegroundColor Cyan

docker exec lakehouse-spark-master /opt/spark/bin/spark-submit `
    --master local[2] `
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
    /opt/spark-apps/silver_writer.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n   OK  Silver writer completed" -ForegroundColor Green
} else {
    Write-Host "`n   ERR Silver writer failed (exit $LASTEXITCODE)" -ForegroundColor Red
}