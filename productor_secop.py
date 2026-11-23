import json
from pathlib import Path
from confluent_kafka import Producer

TOPICO = "secop-contratos-raw"
CARPETA_DATOS = Path("data/raw")

archivos = sorted(CARPETA_DATOS.rglob("*.jsonl"))
if not archivos:
    raise SystemExit("No encontré archivos .jsonl en data/raw")

productor = Producer({"bootstrap.servers": "localhost:9092"})
enviados = 0
errores = 0

def confirmar_entrega(error, mensaje):
    global enviados, errores
    if error:
        errores += 1
        print(f"Error al entregar mensaje: {error}")
    else:
        enviados += 1

for archivo in archivos:
    print(f"Procesando: {archivo}")

    with archivo.open("r", encoding="utf-8") as datos:
        for numero_linea, linea in enumerate(datos, start=1):
            if not linea.strip():
                continue

            try:
                registro = json.loads(linea)
                clave = str(registro["id_registro"])
            except (json.JSONDecodeError, KeyError) as exc:
                errores += 1
                print(f"Registro inválido en {archivo}, línea {numero_linea}: {exc}")
                continue

            while True:
                try:
                    productor.produce(
                        TOPICO,
                        key=clave.encode("utf-8"),
                        value=linea.strip().encode("utf-8"),
                        callback=confirmar_entrega,
                    )
                    break
                except BufferError:
                    productor.poll(1)

            productor.poll(0)

pendientes = productor.flush(30)
print(f"Entregados: {enviados} | Errores: {errores} | Pendientes: {pendientes}")