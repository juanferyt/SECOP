"""Socrata keyset pagination; each committed run has a manifest and cursor."""
import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://www.datos.gov.co/resource/jbjy-vk9h.json'
ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / 'data/state/api_cursor.json'


def session():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retry))
    if os.getenv('SOCRATA_APP_TOKEN'):
        s.headers['X-App-Token'] = os.environ['SOCRATA_APP_TOKEN']
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--page-size', type=int, default=1000)
    ap.add_argument('--max-rows', type=int, default=20000)
    ap.add_argument('--start-date', default='2026-01-01T00:00:00')
    ap.add_argument('--resume', action='store_true', help='Read cursor from last committed run')
    args = ap.parse_args()
    if args.page_size < 1 or args.max_rows < 1:
        ap.error('page-size and max-rows must be positive')
    watermark = json.loads(STATE.read_text(encoding='utf-8')) if args.resume and STATE.exists() else None
    if args.resume and not watermark:
        manifests = sorted((ROOT / 'data/raw').rglob('*.manifest.json'))
        if manifests:
            last = json.loads(manifests[-1].read_text(encoding='utf-8'))
            watermark = {'fecha_de_firma': last['max_fecha_firma'],
                         'id_contrato': last['max_id_contrato']}
    run_time = datetime.now(timezone.utc)
    run_id = run_time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
    directory = ROOT / 'data/raw' / f'fecha_ingesta={run_time:%Y-%m-%d}'
    directory.mkdir(parents=True, exist_ok=True)
    file = directory / f'lote-{run_id}.jsonl'
    temp = file.with_suffix('.jsonl.tmp')
    count = pages = missing = 0
    seen = set()
    cursor = watermark
    has_more = True
    with session() as s, temp.open('w', encoding='utf-8') as out:
        while count < args.max_rows:
            where = f"fecha_de_firma IS NOT NULL AND fecha_de_firma >= '{args.start_date}'"
            if cursor:
                dt = cursor['fecha_de_firma'].replace("'", "''")
                ident = cursor['id_contrato'].replace("'", "''")
                where += (f" AND (fecha_de_firma > '{dt}' OR "
                          f"(fecha_de_firma = '{dt}' AND id_contrato > '{ident}'))")
            params = {'$where': where, '$order': 'fecha_de_firma ASC,id_contrato ASC',
                      '$limit': min(args.page_size, args.max_rows - count)}
            response = s.get(URL, params=params, timeout=(10, 90))
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list):
                raise ValueError('API did not return a list')
            pages += 1
            if not rows:
                has_more = False
                break
            for item in rows:
                date, ident = item.get('fecha_de_firma'), item.get('id_contrato')
                if not date or not ident:
                    missing += 1
                    continue
                if cursor and (date, ident) <= (cursor['fecha_de_firma'], cursor['id_contrato']):
                    raise ValueError('API ordering cursor did not advance')
                cursor = {'fecha_de_firma': date, 'id_contrato': ident}
                if ident in seen:
                    continue
                seen.add(ident)
                obj = {'fuente': 'datos.gov.co', 'dataset': 'jbjy-vk9h',
                       'id_registro': ident, 'fecha_ingesta': run_time.isoformat(),
                       'run_id': run_id, 'payload': item}
                out.write(json.dumps(obj, ensure_ascii=False) + '\n')
                count += 1
            if len(rows) < params['$limit']:
                has_more = False
                break
    if not count:
        temp.unlink()
        print('No new records found; raw zone unchanged')
        return
    temp.replace(file)
    manifest = {'run_id': run_id, 'url': URL, 'parametros': vars(args),
                'fecha_ingesta': run_time.isoformat(), 'registros': count,
                'descartados_sin_id': missing, 'paginas': pages,
                'sha256_jsonl': hashlib.sha256(file.read_bytes()).hexdigest(),
                'archivo': str(file.relative_to(ROOT)).replace('\\', '/'),
                'max_fecha_firma': cursor['fecha_de_firma'],
                'max_id_contrato': cursor['id_contrato'],
                'extraccion_completa': not has_more}
    file.with_suffix('.manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(cursor, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'{count} records in {file.relative_to(ROOT)}; {pages} API pages')


if __name__ == '__main__':
    main()
