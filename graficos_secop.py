import csv
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt
from confluent_kafka import Consumer, TopicPartition

TOPICO = "secop-contratos-raw"
SALIDA = Path("resultados")
SALIDA.mkdir(exist_ok=True)

consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "graficos-secop-local",
    "enable.auto.commit": False,
})

departamentos = defaultdict(list)
modalidades = defaultdict(list)
todos_los_valores = []

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
        valor = Decimal(str(datos["valor_del_contrato"]))

        departamentos[datos["departamento"]].append(valor)
        modalidades[datos["modalidad_de_contratacion"]].append(valor)
        todos_los_valores.append(valor)

        if mensaje.offset() + 1 >= limites[mensaje.partition()]:
            terminadas.add(mensaje.partition())
finally:
    consumidor.close()

def guardar_tabla(nombre, grupos):
    filas = sorted(
        grupos.items(),
        key=lambda elemento: len(elemento[1]),
        reverse=True,
    )
    with (SALIDA / nombre).open("w", newline="", encoding="utf-8-sig") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["categoria", "contratos", "valor_total_cop", "mediana_cop"])
        for categoria, valores in filas:
            escritor.writerow([
                categoria,
                len(valores),
                str(sum(valores, Decimal("0"))),
                str(median(valores)),
            ])
    return filas

por_departamento = guardar_tabla("departamentos.csv", departamentos)
por_modalidad = guardar_tabla("modalidades.csv", modalidades)

def grafico_barras(filas, medida, titulo, etiqueta, archivo, color):
    principales = filas[:10][::-1]
    nombres = [nombre for nombre, _ in principales]
    cantidades = [medida(valores) for _, valores in principales]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(nombres, cantidades, color=color)
    ax.set_title(titulo)
    ax.set_xlabel(etiqueta)
    ax.grid(axis="x", alpha=0.2)
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(SALIDA / archivo, dpi=180, bbox_inches="tight")
    plt.close(fig)

grafico_barras(
    por_departamento,
    lambda valores: len(valores),
    "Contratos por departamento: 10 principales",
    "Número de contratos",
    "contratos_departamento.png",
    "#2563A6",
)

grafico_barras(
    por_departamento,
    lambda valores: float(median(valores)) / 1_000_000,
    "Valor mediano por departamento: 10 con más contratos",
    "Millones de COP",
    "mediana_departamento.png",
    "#1B8A75",
)

grafico_barras(
    por_modalidad,
    lambda valores: len(valores),
    "Contratos por modalidad: 10 principales",
    "Número de contratos",
    "contratos_modalidad.png",
    "#B86A28",
)

valores_ordenados = sorted(todos_los_valores, reverse=True)
n_top = max(1, len(valores_ordenados) // 100)
valor_top = sum(valores_ordenados[:n_top], Decimal("0"))
valor_resto = sum(valores_ordenados[n_top:], Decimal("0"))

fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(
    ["1 % de mayor valor", "99 % restante"],
    [float(valor_top / 1_000_000_000_000),
     float(valor_resto / 1_000_000_000_000)],
    color=["#B54845", "#2563A6"],
)
ax.set_title("Concentración del valor contractual")
ax.set_ylabel("Billones de COP")
ax.grid(axis="y", alpha=0.2)
ax.set_axisbelow(True)
fig.tight_layout()
fig.savefig(SALIDA / "concentracion_valor.png", dpi=180)
plt.close(fig)

print(f"Mensajes analizados: {len(todos_los_valores)}")
print(f"Archivos creados en: {SALIDA.resolve()}")