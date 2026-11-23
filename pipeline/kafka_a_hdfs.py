"""One HDFS file per Kafka run, with duplicate IDs removed per run."""
import json
import subprocess
from collections import defaultdict
from pathlib import Path
from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parents[1]
TOPIC = 'secop-contratos-raw'
STAGING = ROOT / 'data/staging'


def docker(*args):
    return subprocess.run(['docker', 'compose', 'exec', '-T', 'namenode', *args],
                          cwd=ROOT, check=True, text=True, capture_output=True)


def main():
    STAGING.mkdir(parents=True, exist_ok=True)
    c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'hdfs-secop',
                  'enable.auto.commit': False})
    handles, seen = {}, defaultdict(set)
    counts = defaultdict(int)
    skipped = 0
    try:
        partitions = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        starts, ends = [], {}
        for p in partitions:
            low, high = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            starts.append(TopicPartition(TOPIC, p, low)); ends[p] = high
        c.assign(starts)
        done = {p for p, end in ends.items() if end <= starts[p].offset}
        while len(done) < len(ends):
            m = c.poll(20)
            if m is None: raise TimeoutError('Kafka did not deliver data in 20 seconds')
            if m.error(): raise RuntimeError(m.error())
            if m.partition() in done: continue
            obj = json.loads(m.value().decode('utf-8'))
            run = obj['run_id']
            ident = obj['id_registro']
            day = obj['fecha_ingesta'][:10]
            if ident in seen[run]:
                skipped += 1
            else:
                seen[run].add(ident)
                path = STAGING / f'lote-{run}.jsonl'
                if run not in handles: handles[run] = path.open('w', encoding='utf-8')
                handles[run].write(m.value().decode('utf-8') + '\n')
                counts[(run, day)] += 1
            if m.offset() + 1 >= ends[m.partition()]: done.add(m.partition())
    finally:
        for f in handles.values(): f.close()
        c.close()
    for (run, day), count in sorted(counts.items()):
        destination = f'/secop/raw/datos.gov.co/jbjy-vk9h/fecha_ingesta={day}'
        docker('hdfs', 'dfs', '-mkdir', '-p', destination)
        docker('hdfs', 'dfs', '-put', '-f', f'/workspace/data/staging/lote-{run}.jsonl',
               f'{destination}/lote-{run}.jsonl')
        print(f'{destination}/lote-{run}.jsonl: {count} unique messages')
    report = {'unique_messages': sum(counts.values()), 'duplicate_messages_skipped': skipped,
              'files': len(counts)}
    (ROOT / 'resultados').mkdir(exist_ok=True)
    (ROOT / 'resultados/hdfs_carga.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    (ROOT / 'evidencias').mkdir(exist_ok=True)
    (ROOT / 'evidencias/hdfs_ls.txt').write_text(
        docker('hdfs', 'dfs', '-ls', '-R', '/secop/raw').stdout, encoding='utf-8')
    print(report)


if __name__ == '__main__': main()
