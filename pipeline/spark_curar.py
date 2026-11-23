"""Batch Lambda layer: HDFS raw -> validated Parquet -> SQLite indicators."""
import json
import sqlite3
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path('/workspace')
RAW = 'hdfs://namenode:8020/secop/raw/datos.gov.co/jbjy-vk9h'
CURATED = 'hdfs://namenode:8020/secop/curada/contratos'
OUT = ROOT / 'resultados'
OUT.mkdir(exist_ok=True)
spark = (SparkSession.builder.master('local[2]').appName('SECOP-curada')
         .config('spark.driver.memory', '2g').config('spark.sql.shuffle.partitions', '4')
         .config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:8020')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')


def hdfs_bytes(path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).getContentSummary(p).getLength()


try:
    raw = spark.read.json(RAW + '/fecha_ingesta=*/*.jsonl')
    before = raw.count()
    if before == 0: raise ValueError('CRITICAL: no records in HDFS raw')

    raw_counts = {r['departamento']: r['count'] for r in raw.groupBy(
        F.col('payload.departamento').alias('departamento')).count().collect()}
    mr_file = OUT / 'mapreduce_departamento.tsv'
    if mr_file.exists():
        mr_counts = dict(line.rstrip('\n').split('\t', 1) for line in mr_file.open(encoding='utf-8'))
        mr_counts = {k: int(v) for k, v in mr_counts.items()}
        if mr_counts != raw_counts:
            raise ValueError('CRITICAL: MapReduce and Spark raw counts differ')
    else:
        raise FileNotFoundError('Run pipeline/mapreduce.py before Spark comparison')

    selected = raw.select(
        F.trim('id_registro').alias('id_registro'),
        F.trim(F.col('payload.nombre_entidad')).alias('entidad'),
        F.trim(F.col('payload.departamento')).alias('departamento'),
        F.trim(F.col('payload.modalidad_de_contratacion')).alias('modalidad'),
        F.to_timestamp('payload.fecha_de_firma').alias('fecha_firma'),
        F.col('payload.valor_del_contrato').cast('decimal(20,0)').alias('valor_cop'),
        F.to_timestamp('fecha_ingesta').alias('fecha_ingesta'),
        F.col('run_id'),
    )
    invalid = (F.col('id_registro').isNull() | (F.col('id_registro') == '') |
               F.col('departamento').isNull() | (F.col('departamento') == '') |
               F.col('fecha_firma').isNull() | F.col('valor_cop').isNull() |
               (F.col('valor_cop') < 0))
    discarded = selected.filter(invalid).count()
    clean = selected.filter(~invalid)
    ordering = Window.partitionBy('id_registro').orderBy(F.desc('fecha_ingesta'), F.desc('run_id'))
    unique = clean.withColumn('rank', F.row_number().over(ordering)).filter('rank = 1').drop('rank')
    after = unique.count()
    duplicates = before - discarded - after
    report = {'raw_rows': before, 'invalid_discarded': discarded,
              'duplicates_removed': duplicates, 'curated_rows': after,
              'mapreduce_spark_match': True, 'zero_values_kept': unique.filter('valor_cop = 0').count(),
              'critical_invalid_threshold_pct': 5}
    (OUT / 'calidad_pipeline.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    if discarded / before > 0.05:
        raise ValueError('CRITICAL: more than 5% of raw records have invalid required fields')
    if after == 0 or unique.filter('id_registro IS NULL OR departamento IS NULL').count():
        raise ValueError('CRITICAL: invalid curated data; pipeline stopped')
    if unique.select('id_registro').distinct().count() != after:
        raise ValueError('CRITICAL: duplicate curated IDs; pipeline stopped')

    # Department partitions serve geographic queries and avoid a single huge output partition.
    unique.write.mode('overwrite').partitionBy('departamento').parquet(CURATED)
    size_report = {'raw_jsonl_bytes': hdfs_bytes(RAW), 'curated_parquet_bytes': hdfs_bytes(CURATED)}
    (OUT / 'tamano_jsonl_parquet.json').write_text(json.dumps(size_report, indent=2), encoding='utf-8')

    aggregate = lambda group: unique.groupBy(group).agg(
        F.count('*').alias('contratos'), F.sum('valor_cop').alias('valor_total_cop'),
        F.expr('percentile_approx(valor_cop, 0.5, 10000)').alias('mediana_aprox_cop'))
    by_dept = aggregate('departamento').collect()
    by_mode = aggregate('modalidad').collect()
    by_date = unique.groupBy(F.to_date('fecha_firma').alias('fecha')).agg(
        F.count('*').alias('contratos'), F.sum('valor_cop').alias('valor_total_cop')).collect()
    db_path = ROOT / 'data/service.db'
    with sqlite3.connect(db_path) as db:
        db.execute('CREATE TABLE IF NOT EXISTS departamento (nombre TEXT PRIMARY KEY, contratos INTEGER, valor_total_cop INTEGER, mediana_aprox_cop INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS modalidad (nombre TEXT PRIMARY KEY, contratos INTEGER, valor_total_cop INTEGER, mediana_aprox_cop INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS fecha (dia TEXT PRIMARY KEY, contratos INTEGER, valor_total_cop INTEGER)')
        for tab in ('departamento', 'modalidad', 'fecha'): db.execute(f'DELETE FROM {tab}')
        db.executemany('INSERT INTO departamento VALUES (?,?,?,?)', [
            (r.departamento, r.contratos, int(r.valor_total_cop), int(r.mediana_aprox_cop)) for r in by_dept])
        db.executemany('INSERT INTO modalidad VALUES (?,?,?,?)', [
            (r.modalidad, r.contratos, int(r.valor_total_cop), int(r.mediana_aprox_cop)) for r in by_mode])
        db.executemany('INSERT INTO fecha VALUES (?,?,?)', [
            (r.fecha.isoformat(), r.contratos, int(r.valor_total_cop)) for r in by_date])
    print(report)
    print(size_report)
finally:
    spark.stop()
