import json
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation

from confluent_kafka import Consumer, TopicPartition

TOPICO = "secop-contratos-raw"
CAMPOS = (
    "nombre_entidad",
    "departamento",
    "modalidad_de_contratacion",
    "proveedor_adjudicado",
    "valor_del_contrato",
    "fecha_de_firma",
)

consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "calidad-secop-local",
    "enable.auto.commit": False,
})

vistos = set()
faltantes = Counter()
mensajes_leidos = 0
json_invalidos = 0
ids_faltantes = 0
ids_duplicados = 0
valores_invalidos = 0
valores_negativos = 0
valores_cero = 0
fechas_invalidas = 0

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

        particion = mensaje.partition()
        if particion in terminadas:
            continue

        mensajes_leidos += 1

        try:
            registro = json.loads(mensaje.value().decode("utf-8"))
            if not isinstance(registro, dict):
                raise ValueError("El registro no es un objeto JSON")
        except (ValueError, UnicodeDecodeError):
            json_invalidos += 1
            registro = {}

        identificador = registro.get("id_registro")
        if identificador in (None, ""):
            ids_faltantes += 1
        elif identificador in vistos:
            ids_duplicados += 1
        else:
            vistos.add(identificador)

        datos = registro.get("payload")
        if not isinstance(datos, dict):
            datos = {}

        for campo in CAMPOS:
            if datos.get(campo) in (None, ""):
                faltantes[campo] += 1

        valor = datos.get("valor_del_contrato")
        if valor not in (None, ""):
            try:
                numero = Decimal(str(valor))
                if not numero.is_finite():
                    valores_invalidos += 1
                elif numero < 0:
                    valores_negativos += 1
                elif numero == 0:
                    valores_cero += 1
            except InvalidOperation:
                valores_invalidos += 1

        fecha = datos.get("fecha_de_firma")
        if fecha not in (None, ""):
            try:
                datetime.fromisoformat(str(fecha).replace("Z", "+00:00"))
            except ValueError:
                fechas_invalidas += 1

        if mensaje.offset() + 1 >= limites[particion]:
            terminadas.add(particion)
finally:
    consumidor.close()

print(f"Mensajes revisados: {mensajes_leidos}")
print(f"JSON inválidos: {json_invalidos}")
print(f"ID faltantes: {ids_faltantes}")
print(f"ID duplicados: {ids_duplicados}")
print(f"Valores inválidos: {valores_invalidos}")
print(f"Valores negativos: {valores_negativos}")
print(f"Valores iguales a cero: {valores_cero}")
print(f"Fechas de firma inválidas: {fechas_invalidas}")

print("\nCampos vacíos:")
for campo in CAMPOS:
    print(f"  {campo}: {faltantes[campo]}")