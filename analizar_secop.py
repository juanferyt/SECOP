import json
from collections import Counter
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer, TopicPartition

TOPICO = "secop-contratos-raw"

consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analisis-secop-local",
    "enable.auto.commit": False,
})

por_departamento = Counter()
por_modalidad = Counter()
valor_total = Decimal("0")
procesados = 0
invalidos = 0

try:
    metadata = consumidor.list_topics(TOPICO, timeout=10)
    particiones = metadata.topics[TOPICO].partitions
    inicio = []
    limites = {}

    for numero in particiones:
        particion = TopicPartition(TOPICO, numero)
        minimo, maximo = consumidor.get_watermark_offsets(particion, timeout=10)
        inicio.append(TopicPartition(TOPICO, numero, minimo))
        limites[numero] = maximo

    # Lee desde el comienzo hasta la posición final observada al iniciar.
    consumidor.assign(inicio)
    terminadas = {p for p, limite in limites.items() if limite == 0}

    while len(terminadas) < len(limites):
        mensaje = consumidor.poll(10)

        if mensaje is None:
            raise TimeoutError("Kafka no entregó mensajes durante 10 segundos")
        if mensaje.error():
            raise RuntimeError(mensaje.error())

        particion = mensaje.partition()
        if particion in terminadas:
            continue

        try:
            registro = json.loads(mensaje.value().decode("utf-8"))
            datos = registro["payload"]

            por_departamento[datos.get("departamento") or "Sin dato"] += 1
            por_modalidad[datos.get("modalidad_de_contratacion") or "Sin dato"] += 1

            valor = datos.get("valor_del_contrato")
            if valor not in (None, ""):
                valor_total += Decimal(str(valor))

            procesados += 1
        except (ValueError, KeyError, InvalidOperation):
            invalidos += 1

        if mensaje.offset() + 1 >= limites[particion]:
            terminadas.add(particion)
finally:
    consumidor.close()

print(f"\nContratos procesados: {procesados}")
print(f"Registros inválidos: {invalidos}")
print(f"Valor total reportado: ${valor_total:,.0f} COP")

print("\n10 departamentos con más contratos:")
for nombre, cantidad in por_departamento.most_common(10):
    print(f"  {nombre}: {cantidad}")

print("\n10 modalidades más frecuentes:")
for nombre, cantidad in por_modalidad.most_common(10):
    print(f"  {nombre}: {cantidad}")