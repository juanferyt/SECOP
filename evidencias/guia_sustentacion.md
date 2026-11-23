# Ensayo de sustentación

Cada integrante debe poder explicar el recorrido completo, no solo su fase asignada. Practiquen alternándose y respondan sin leer el README:

1. **Pregunta y fuente:** ¿qué mide el proyecto, quién usaría el indicador y qué API pública se consultó? ¿Qué identifica `id_registro`?
2. **Raw e ingesta:** ¿qué contiene el sobre JSON, cómo se pagina la API y qué registra el manifiesto?
3. **Kafka:** ¿por qué la clave es el ID del contrato, qué evita duplicados al reejecutar el productor y cuántos mensajes nuevos hubo?
4. **HDFS:** ¿cómo se particionan los archivos por fecha/lote y cuántos registros quedaron en cada lote?
5. **MapReduce y Spark:** ¿qué emite el mapper, qué agrega el reducer y cómo se verificó la coincidencia de 20.000 filas en 30 departamentos?
6. **Curada y calidad:** ¿qué validaciones se ejecutan, cómo se tratan los duplicados/tipos y dónde se guarda Parquet?
7. **Streaming:** ¿qué ventana, watermark y checkpoint se usan? ¿Qué conteos se comparan después del reinicio?
8. **Servicio y tablero:** ¿qué tablas alimentan las visualizaciones y por qué el tablero no vuelve a calcular los indicadores?

**Criterio de ensayo:** cada integrante debe poder dibujar el flujo raw → Kafka/HDFS → MapReduce/Spark → Parquet/SQLite → tablero y justificar una decisión de cada fase, además de explicar por qué la segunda ejecución no duplicó contratos.
