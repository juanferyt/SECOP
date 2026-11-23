"""Skip IDs already present in Kafka; demonstrates safe re-run for this lab."""
import json
from pathlib import Path
from confluent_kafka import Consumer, Producer, TopicPartition

TOPIC = 'secop-contratos-raw'
ROOT = Path(__file__).resolve().parents[1]


def existing_ids():
    c = Consumer({'bootstrap.servers': 'localhost:9092',
                  'group.id': 'scan-ids-secop', 'enable.auto.commit': False})
    keys = set()
    try:
        partitions = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        starts, ends = [], {}
        for part in partitions:
            low, high = c.get_watermark_offsets(TopicPartition(TOPIC, part), timeout=10)
            starts.append(TopicPartition(TOPIC, part, low))
            ends[part] = high
        c.assign(starts)
        done = {p for p, end in ends.items() if end <= starts[p].offset}
        while len(done) < len(ends):
            m = c.poll(20)
            if m is None: raise TimeoutError('Kafka scan timed out')
            if m.error(): raise RuntimeError(m.error())
            if m.partition() in done: continue
            keys.add(m.key().decode('utf-8') if m.key() else json.loads(m.value())['id_registro'])
            if m.offset() + 1 >= ends[m.partition()]: done.add(m.partition())
        return keys
    finally:
        c.close()


def main():
    seen = existing_ids()
    before = len(seen)
    producer = Producer({'bootstrap.servers': 'localhost:9092',
                         'enable.idempotence': True, 'acks': 'all'})
    state = {'delivered': 0, 'failed': 0}

    def delivered(err, _):
        state['failed' if err else 'delivered'] += 1

    for path in sorted((ROOT / 'data/raw').rglob('*.jsonl')):
        with path.open(encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                obj = json.loads(line)
                key = obj['id_registro']
                if key in seen: continue
                # Local id set guards repeats between files in the same invocation.
                seen.add(key)
                while True:
                    try:
                        producer.produce(TOPIC, key=key.encode(), value=line.strip().encode(),
                                         callback=delivered)
                        break
                    except BufferError: producer.poll(1)
                producer.poll(0)
    pending = producer.flush(60)
    print(f'IDs already present: {before}; new delivered: {state["delivered"]}; '
          f'failed: {state["failed"]}; pending: {pending}')
    if state['failed'] or pending: raise SystemExit('Kafka delivery failed; retry after diagnosing')


if __name__ == '__main__':
    main()
