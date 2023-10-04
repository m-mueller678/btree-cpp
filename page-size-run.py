#!/usr/bin/env python

import subprocess
import sys
from math import floor, log10
from random import choices, randrange, random
from uuid import uuid4

while (True):
    ycsb = choices(population=[3, 5], weights=[3, 1])[0]
    data = choices(population=['int', 'data/urls', 'data/wiki'], weights=[1,2, 2])[0]
    avg_key_size = {'data/urls': 62.280, 'data/wiki': 22.555, 'int': 4}[data]
    max_key_count = {'data/urls': 6300000, 'data/wiki': 15000000, 'int': 4e9}[data]
    lower = 8 if data == 'int' else 10
    psl_exp = 12  # randrange(lower, 16)
    psl = 2 ** psl_exp
    pl = randrange(0, 256)
    target_total_size = 10 ** (random()*2.5+6)
    density = 1 / 2 ** random()
    key_count = floor(target_total_size / (pl+avg_key_size))
    if key_count>max_key_count:
        continue
    config = choices(population='dense1 hash hints dense2 baseline prefix'.split())[0]
    if (config == 'dense1' or config == 'dense2') and data != 'int':
        continue
    env = {
        'RUN_ID': uuid4(),
        'YCSB_VARIANT': ycsb,
        'SCAN_LENGTH': 100,
        'OP_COUNT': '1e7',
        'DATA': data,
        'KEY_COUNT': key_count,
        'PAYLOAD_SIZE': pl,
        'ZIPF': -1,
        'DENSITY': density,
    }
    env = {k: str(env[k]) for k in env}
    path = f'page-size-builds/-DPS_I=4096 -DPS_L={psl}/{config}-n3-ycsb'
    try:
        print(path, env)
        with open('page-size-random-4.csv', 'a') as f:
            subprocess.run(path, stdout=f, env=env, check=True, timeout=120)
    except Exception as e:
        print(path, env, e, file=sys.stderr)
