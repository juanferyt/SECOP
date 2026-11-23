# Evidencias reproducibles

Estas salidas corresponden al pipeline SECOP y documentan cada etapa solicitada:

| Fase | Archivo | Qué demuestra |
|---|---|---|
| Kafka | `evidencia_ingesta_kafka.png`, `kafka_hdfs_ingesta.json` | Tópico, productor idempotente y 20.000 mensajes enviados a HDFS en dos lotes. |
| HDFS | `hdfs_ls.txt`, `hdfs_todo.txt` | Archivos raw particionados por fecha y estructura HDFS completa. |
| MapReduce/Spark | `mapreduce_departamento.tsv`, `comparacion_mapreduce_spark.json` | Conteos por departamento; 20.000 filas y coincidencia de las 30 categorías. |
| Reinicio | `idempotencia.txt`, `streaming_reinicio.json` | Conteos batch/streaming sin incremento tras la segunda ejecución y reinicio. |
| Calidad | `calidad_pipeline.json` | Filas raw, descartes, deduplicación y validación del resultado curado. |

`../muestra/` contiene 20 registros raw con su manifiesto SHA-256 y CSV exportados de las tablas de servicio. No se incluye el conjunto raw completo ni el archivo SQLite operativo.

Usen `guia_sustentacion.md` para ensayar que ambos integrantes puedan recorrer el pipeline completo.
