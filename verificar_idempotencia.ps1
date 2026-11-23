$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Snapshot {
  py -c "import sqlite3; c=sqlite3.connect('data/service.db'); print(c.execute('select sum(contratos) from departamento').fetchone()[0], c.execute('select sum(contratos) from stream_hour').fetchone()[0])"
  if ($LASTEXITCODE -ne 0) { throw 'Could not read service counts' }
}

$before = Snapshot
py pipeline/publicar_incremental.py
if ($LASTEXITCODE -ne 0) { throw 'Producer failed' }
py pipeline/kafka_a_hdfs.py
if ($LASTEXITCODE -ne 0) { throw 'HDFS consumer failed' }
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/pipeline/spark_curar.py
if ($LASTEXITCODE -ne 0) { throw 'Spark batch failed' }
docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/pipeline/streaming.py
if ($LASTEXITCODE -ne 0) { throw 'Spark streaming failed' }
$after = Snapshot
$summary = "Before (batch, streaming): $before`nAfter (batch, streaming): $after`n"
$summary | Tee-Object -FilePath evidencias/idempotencia.txt
if ($before -ne $after) { throw 'Idempotency check failed: counts changed' }
Write-Host 'Verified: rerun did not duplicate service records' -ForegroundColor Green
