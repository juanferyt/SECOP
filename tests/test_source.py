"""Ground-truth tests for supplied lot manifests and mapper/reducer output."""
import collections
import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SourceChecks(unittest.TestCase):
    def test_manifest_hashes_and_unique_ids(self):
        manifests = sorted((ROOT / 'data/raw').rglob('*.manifest.json'))
        if not manifests:
            self.skipTest('Only source sample checked out; large raw data excluded from Git')
        ids = set()
        total = 0
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            lot = ROOT / manifest['archivo']
            self.assertEqual(hashlib.sha256(lot.read_bytes()).hexdigest(), manifest['sha256_jsonl'])
            with lot.open(encoding='utf-8') as stream:
                rows = [json.loads(line) for line in stream]
            self.assertEqual(len(rows), manifest['registros'])
            for row in rows:
                self.assertNotIn(row['id_registro'], ids)
                ids.add(row['id_registro'])
            total += len(rows)
        self.assertGreaterEqual(total, 20000)

    def test_mapreduce_matches_direct_count(self):
        lines = (ROOT / 'muestra/raw_muestra.jsonl').read_text(encoding='utf-8').splitlines()
        expected = collections.Counter(json.loads(line)['payload']['departamento'] for line in lines)
        mapper = subprocess.run([sys.executable, str(ROOT / 'pipeline/mapper.py')],
                                input='\n'.join(lines)+'\n', text=True, capture_output=True, check=True)
        reducer = subprocess.run([sys.executable, str(ROOT / 'pipeline/reducer.py')],
                                 input='\n'.join(sorted(mapper.stdout.splitlines()))+'\n',
                                 text=True, capture_output=True, check=True)
        actual = dict((name, int(count)) for name, count in (
            line.split('\t') for line in reducer.stdout.splitlines()))
        self.assertEqual(actual, expected)


if __name__ == '__main__': unittest.main()
