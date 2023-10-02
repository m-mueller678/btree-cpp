#!/usr/bin/env python

import subprocess
import sys
from math import floor
from random import choices, randrange, random
from uuid import uuid4

while (True):
    ycsb = choices(population=[3, 5], weights=[3, 1])[0]
    data = choices(population=['int', 'data/urls', 'data/wiki'],weights=[3,1,1])[0]
    lower = 8 if data == 'int' else 10
    psl_exp = randrange(lower, 16)
    psl = 2 ** psl_exp
    pl = randrange(0, 256)
    density = 1 / 2 ** random()
    config = choices(population='dense1 hash hints'.split())[0]
    if config == 'dense1' and data != 'int':
        continue
    env = {
        'RUN_ID': uuid4(),
        'YCSB_VARIANT': ycsb,
        'SCAN_LENGTH': 100,
        'OP_COUNT': '1e7',
        'DATA': data,
        'KEY_COUNT': floor(3e8 / ({'data/urls': 62.280, 'data/wiki': 22.555, 'int': 4}[data] + pl)),
        'PAYLOAD_SIZE': pl,
        'ZIPF': -1,
        'DENSITY': density,
    }
    env = {k: str(env[k]) for k in env}
    path = f'page-size-builds/-DPS_I=4096 -DPS_L={psl}/{config}-n3-ycsb'
    try:
        print(path, env)
        with open('page-size-random-2.csv', 'a') as f:
            subprocess.run(path, stdout=f, env=env, check=True, timeout=120)
    except Exception as e:
        print(path, env, e, file=sys.stderr)
