#!/usr/bin/env python
import fcntl
import subprocess
import sys
from math import floor, log10
from random import choices, randrange, random
from uuid import uuid4

STDOUT = 'random-hot.csv'
STDERR = f'errlog-{STDOUT}'


# parallel -j16 -- ./page-size-run.py {1} ::: $(seq 1 16)

def append(path, content):
    if content == '':
        return
    with open(path, 'a') as f:
        fcntl.lockf(f, fcntl.LOCK_EX)
        try:
            try:
                f.write(content)
            finally:
                f.flush()
        finally:
            fcntl.lockf(f, fcntl.F_UNLCK)


while (True):
    #ycsb = choices(population=[3, 5], weights=[3, 1])[0]
    ycsb = 3
    data = choices(population=['int', 'data/urls-short', 'data/wiki'], weights=[1, 1, 1])[0]
    avg_key_size = {'data/urls': 62.280, 'data/urls-short':62.204,'data/wiki': 22.555, 'int': 4}[data]
    max_key_count = {'data/urls': 6300000,'data/urls-short': 6300000, 'data/wiki': 15000000, 'int': 4e9}[data]
    lower = 8 if data == 'int' else 10
    psl_exp = 12  # randrange(lower, 16)
    psl = 2 ** psl_exp
    pl = randrange(0, 17)
    target_total_size = 10 ** (random() * 0.7 + 8)
    density = 1 / 2 ** random()
    key_count = floor(target_total_size / (pl + avg_key_size))
    if key_count > max_key_count:
        continue
    # config = choices(population='dense1 hash hints dense2 baseline prefix'.split())[0]
    config = 'hot'
    # config = choices(population='hash hints'.split())[0]
    if (config == 'dense1' or config == 'dense2') and data != 'int':
        continue
    path = f'page-size-builds/-DPS_I=4096 -DPS_L={psl}/{config}-n3-ycsb'
    env = {
        '_path': path,
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
    try:
        process = subprocess.Popen(path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f'nonzero return code: {process.returncode}, stderr: {stderr}')
    except Exception as e:
        process.kill()
        stdout = ''
        stderr = f'exception: {e}\n'
    append(STDOUT, stdout)
    append(STDERR, f'{env}\n{stderr}')
