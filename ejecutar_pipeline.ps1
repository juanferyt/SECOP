$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function RunStep {
  param([string]$Name, [scriptblock]$Action)
  Write-Host "`n== $Name ==" -ForegroundColor Cyan
  & $Action
  if ($LASTEXITCODE -ne 0) { throw "Failed: $Name (exit $LASTEXITCODE)" }
}

New-Item -ItemType Directory -Force evidencias, resultados | Out-Null
RunStep 'Docker HDFS and Spark' { docker compose up -d }
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
  try {
    $status = docker compose exec -T namenode hdfs dfsadmin -report 2>$null
    if ($LASTEXITCODE -eq 0 -and (($status -join " ") -match 'Live datanodes \(1\)')) {
      $ready = $true; break
    }
  } catch { }
  Start-Sleep -Seconds 3
}
if (-not $ready) { throw 'HDFS did not become ready; run docker compose ps -a and docker compose logs namenode datanode' }

RunStep 'Kafka publish only new IDs' { py pipeline/publicar_incremental.py }
RunStep 'Kafka consumer -> HDFS, one file per batch' { py pipeline/kafka_a_hdfs.py }
RunStep 'Hadoop Streaming MapReduce' { py pipeline/mapreduce.py }
RunStep 'PySpark curate, compare, Parquet and SQLite' {
  docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/pipeline/spark_curar.py
}
RunStep 'Structured Streaming with checkpoint' {
  docker compose exec -T spark /opt/spark/bin/spark-submit /workspace/pipeline/streaming.py
}
RunStep 'Save HDFS evidence' {
  docker compose exec -T namenode hdfs dfs -ls -R /secop | Out-File -Encoding utf8 evidencias/hdfs_todo.txt
}
Write-Host 'Pipeline completed. Start dashboard: py -m streamlit run tablero.py' -ForegroundColor Green
