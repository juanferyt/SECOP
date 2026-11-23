#!/usr/bin/env python3
import sys

current = None
total = 0
for line in sys.stdin:
    name, n = line.rstrip('\n').split('\t', 1)
    if current is not None and name != current:
        sys.stdout.write('{}\t{}\n'.format(current, total))
        total = 0
    current = name
    total += int(n)
if current is not None:
    sys.stdout.write('{}\t{}\n'.format(current, total))
