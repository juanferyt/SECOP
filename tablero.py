"""Three precomputed service indicators, read-only SQLite dashboard."""
import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

DB = Path(__file__).parent / 'data/service.db'
st.set_page_config(page_title='Contratos SECOP', layout='wide')
st.title('¿Cómo se distribuyen los contratos de la muestra SECOP por región, modalidad y fecha?')
st.caption('Fuente: datos.gov.co, conjunto jbjy-vk9h. Muestra de 20.000 registros ingeridos el 20 de noviembre de 2026. Los resultados no representan el universo SECOP.')
if not DB.exists():
    st.error('No existe la zona de servicio. Ejecuta pipeline/spark_curar.py.')
    st.stop()
with sqlite3.connect(f'file:{DB}?mode=ro', uri=True) as db:
    departments = db.execute('SELECT nombre, contratos, valor_total_cop, mediana_aprox_cop FROM departamento ORDER BY contratos DESC').fetchall()
    modes = db.execute('SELECT nombre, contratos FROM modalidad ORDER BY contratos DESC').fetchall()
    days = db.execute('SELECT dia, contratos FROM fecha ORDER BY dia').fetchall()
    try:
        fast = db.execute('SELECT COALESCE(SUM(contratos),0) FROM stream_hour').fetchone()[0]
    except sqlite3.OperationalError:
        fast = None
if not departments or not days:
    st.error('Las tablas de servicio están vacías. Ejecuta la zona curada primero.')
    st.stop()
total = sum(r[1] for r in departments)
amount = sum(r[2] for r in departments)
st.caption(f'Fecha de firma máxima de la muestra: {days[-1][0]}. Indicadores preparados por PySpark y guardados en SQLite.')
c1, c2, c3 = st.columns(3)
c1.metric('Contratos', f'{total:,}'.replace(',', '.'))
c2.metric('Valor contractual total COP', f'${amount:,.0f}'.replace(',', '.'))
c3.metric('Departamentos', len(departments))
st.subheader('1. Contratos por departamento')
st.bar_chart(pd.DataFrame([{'Departamento': n, 'Contratos': c} for n, c, _, _ in departments[:10]]),
             x='Departamento', y='Contratos', horizontal=True)
st.subheader('2. Contratos por modalidad')
st.bar_chart(pd.DataFrame([{'Modalidad': n, 'Contratos': c} for n, c in modes]),
             x='Modalidad', y='Contratos', horizontal=True)
st.subheader('3. Contratos según fecha de firma')
st.line_chart(pd.DataFrame([{'Fecha': day, 'Contratos': count} for day, count in days]),
              x='Fecha', y='Contratos')
st.subheader('Lectura de los valores')
st.dataframe([{'Departamento': n, 'Contratos': c, 'Valor total COP': v,
               'Mediana aproximada COP': med} for n, c, v, med in departments],
             hide_index=True, use_container_width=True)
if fast is not None:
    st.caption(f'Capa rápida por ventana de ingesta, con watermark y checkpoint: {fast:,} eventos agregados.'.replace(',', '.'))
