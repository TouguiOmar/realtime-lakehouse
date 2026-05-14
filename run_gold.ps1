# ─────────────────────────────────────────────
#  run_gold.ps1  —  Run Gold aggregations
# ─────────────────────────────────────────────
Write-Host "`n>> Running Gold writer..." -ForegroundColor Cyan

docker exec lakehouse-spark-master /opt/spark/bin/spark-submit `
    --master local[2] `
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
    /opt/spark-apps/gold_writer.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n   OK  Gold writer completed" -ForegroundColor Green
} else {
    Write-Host "`n   ERR Gold writer failed (exit $LASTEXITCODE)" -ForegroundColor Red
}