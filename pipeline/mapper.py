#!/usr/bin/env python3
import json
import sys


def write_department(department):
    if sys.version_info[0] < 3:
        line = u'{}\t1\n'.format(department).encode('utf-8')
    else:
        line = '{}\t1\n'.format(department)
    sys.stdout.write(line)


for line in sys.stdin:
    try:
        obj = json.loads(line)
        department = (obj.get('payload') or {}).get('departamento') or 'Sin dato'
        write_department(department)
    except (ValueError, TypeError):
        sys.stderr.write('INVALID\t1\n')
