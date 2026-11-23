import json
from decimal import Decimal
from statistics import median

from confluent_kafka import Consumer, TopicPartition

TOPICO = "secop-contratos-raw"
consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "valores-secop-local",
    "enable.auto.commit": False,
})

contratos = []

try:
    metadata = consumidor.list_topics(TOPICO, timeout=10)
    inicio = []
    limites = {}

    for numero in metadata.topics[TOPICO].partitions:
        particion = TopicPartition(TOPICO, numero)
        minimo, maximo = consumidor.get_watermark_offsets(particion, timeout=10)
        inicio.append(TopicPartition(TOPICO, numero, minimo))
        limites[numero] = maximo

    consumidor.assign(inicio)
    terminadas = {p for p, limite in limites.items() if limite <= inicio[p].offset}

    while len(terminadas) < len(limites):
        mensaje = consumidor.poll(10)
        if mensaje is None:
            raise TimeoutError("No llegaron mensajes durante 10 segundos")
        if mensaje.error():
            raise RuntimeError(mensaje.error())

        if mensaje.partition() in terminadas:
            continue

        registro = json.loads(mensaje.value().decode("utf-8"))
        datos = registro["payload"]

        contratos.append({
            "id": registro["id_registro"],
            "valor": Decimal(str(datos["valor_del_contrato"])),
            "entidad": datos.get("nombre_entidad", ""),
            "departamento": datos.get("departamento", ""),
            "modalidad": datos.get("modalidad_de_contratacion", ""),
        })

        if mensaje.offset() + 1 >= limites[mensaje.partition()]:
            terminadas.add(mensaje.partition())
finally:
    consumidor.close()

ordenados = sorted(contratos, key=lambda c: c["valor"], reverse=True)
ceros = [c for c in contratos if c["valor"] == 0]
total = sum((c["valor"] for c in contratos), Decimal("0"))
cantidad_top = max(1, len(contratos) // 100)
total_top = sum((c["valor"] for c in ordenados[:cantidad_top]), Decimal("0"))

print(f"Contratos analizados: {len(contratos)}")
print(f"Valor mediano: ${median(c['valor'] for c in contratos):,.0f} COP")
print(f"Valor total: ${total:,.0f} COP")
print(
    f"Participación del 1 % de mayor valor "
    f"({cantidad_top} contratos): {total_top / total:.2%}"
)

print("\nContratos con valor cero:")
for c in ceros:
    print(f"  {c['id']} | {c['departamento']} | {c['entidad']}")

print("\n10 contratos de mayor valor:")
for c in ordenados[:10]:
    print(f"  ${c['valor']:,.0f} | {c['id']} | {c['departamento']}")