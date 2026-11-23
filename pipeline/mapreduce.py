"""Hadoop Streaming on HDFS; results saved for Spark comparison."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ['docker', 'compose', 'exec', '-T', 'namenode']


def call(*args):
    try:
        return subprocess.run([*BASE, *args], cwd=ROOT, check=True,
                              capture_output=True, text=True,
                              encoding='utf-8').stdout
    except subprocess.CalledProcessError as error:
        if error.stdout:
            sys.stdout.write(error.stdout)
        if error.stderr:
            sys.stderr.write(error.stderr)
        raise


def main():
    shell = r'''set -e
jar=$(find /opt/hadoop/share/hadoop/tools/lib -name 'hadoop-streaming*.jar' | head -n 1)
test -n "$jar"
hdfs dfs -rm -r -f /secop/mapreduce/departamento >/dev/null 2>&1 || true
hadoop jar "$jar" -D mapreduce.framework.name=local \
  -files /workspace/pipeline/mapper.py,/workspace/pipeline/reducer.py \
  -mapper 'python mapper.py' -reducer 'python reducer.py' \
  -input '/secop/raw/datos.gov.co/jbjy-vk9h/fecha_ingesta=*/*.jsonl' \
  -output /secop/mapreduce/departamento
'''
    call('bash', '-lc', shell)
    result = call('hdfs', 'dfs', '-cat', '/secop/mapreduce/departamento/part-*')
    out = ROOT / 'resultados/mapreduce_departamento.tsv'
    out.parent.mkdir(exist_ok=True)
    out.write_text(result, encoding='utf-8')
    print(f'MapReduce total: {sum(int(line.rsplit(chr(9), 1)[1]) for line in result.splitlines())}')
    print(f'Output: {out}')


if __name__ == '__main__': main()
