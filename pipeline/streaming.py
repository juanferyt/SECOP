"""Spark Structured Streaming from arriving JSONL batches (allowed folder source)."""
import json
import shutil
import sqlite3
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path('/workspace')
SOURCE = ROOT / 'data/stream_in'
CHECKPOINT = ROOT / 'data/checkpoints/secop_hour'
SOURCE.mkdir(parents=True, exist_ok=True)
CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
DB = ROOT / 'data/service.db'

# Copy only NEW lot filenames. File source + checkpoint remember previously seen files.
added = []
for original in sorted((ROOT / 'data/raw').rglob('*.jsonl')):
    target = SOURCE / original.name
    if not target.exists():
        shutil.copy2(original, target)
        added.append(target.name)

with sqlite3.connect(DB) as db:
    db.execute('CREATE TABLE IF NOT EXISTS stream_hour (inicio TEXT, fin TEXT, departamento TEXT, contratos INTEGER, PRIMARY KEY (inicio, fin, departamento))')
    previous = db.execute('SELECT COALESCE(SUM(contratos),0) FROM stream_hour').fetchone()[0]

schema = T.StructType([
    T.StructField('id_registro', T.StringType()),
    T.StructField('fecha_ingesta', T.StringType()),
    T.StructField('payload', T.StructType([
        T.StructField('departamento', T.StringType()),
    ])),
])
spark = (SparkSession.builder.master('local[2]').appName('SECOP-streaming')
         .config('spark.driver.memory', '2g').config('spark.sql.shuffle.partitions', '4')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')


def save_batch(batch, batch_id):
    rows = batch.collect()
    with sqlite3.connect(DB) as db:
        db.executemany('INSERT INTO stream_hour VALUES (?,?,?,?) ON CONFLICT(inicio,fin,departamento) DO UPDATE SET contratos=excluded.contratos', [
            (r['window']['start'].isoformat(), r['window']['end'].isoformat(),
             r['departamento'], r['count']) for r in rows])
    print(f'batch {batch_id}: {len(rows)} aggregates persisted')


try:
    incoming = (spark.readStream.schema(schema).option('maxFilesPerTrigger', 1)
                .json(str(SOURCE)))
    events = (incoming.select(F.to_timestamp('fecha_ingesta').alias('event_time'),
                              F.col('payload.departamento').alias('departamento'))
              .filter('event_time IS NOT NULL AND departamento IS NOT NULL'))
    counts = (events.withWatermark('event_time', '1 day')
              .groupBy(F.window('event_time', '1 hour'), 'departamento').count())
    query = (counts.writeStream.outputMode('update').foreachBatch(save_batch)
             .option('checkpointLocation', str(CHECKPOINT))
             .trigger(availableNow=True).start())
    query.awaitTermination()
finally:
    spark.stop()

with sqlite3.connect(DB) as db:
    current = db.execute('SELECT COALESCE(SUM(contratos),0) FROM stream_hour').fetchone()[0]
report = {'new_input_files': added, 'contracts_before': previous,
          'contracts_after': current, 'delta': current - previous,
          'checkpoint': str(CHECKPOINT)}
out = ROOT / 'evidencias/streaming_reinicio.json'
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(report)
