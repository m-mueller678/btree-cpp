#!/usr/bin/env python

import subprocess
from math import floor


def run(i,ycsb,data,leaf,inner,config,pl):
    with open('page-size-cpl.csv','a') as f:
        env = {
            'RUN_ID':i,
            'YCSB_VARIANT':ycsb,
            'SCAN_LENGTH':100,
            'OP_COUNT':'1e7',
            'DATA': data,
            'KEY_COUNT': floor(3e8 / ( { 'data/urls': 62.280,'data/wiki' : 22.555,'int': 4}[data] + pl )),
            'PAYLOAD_SIZE':pl,
            'ZIPF':-1,
        }
        env = {k:str(env[k]) for k in env}
        path=f'page-size-builds/-DPS_I={inner} -DPS_L={leaf}/{config}-n3-ycsb'
        result = subprocess.run(path, stdout=f, env=env)
        if result.returncode!=0:
            print(path,env,result)

for i in range(1000):
    for ycsb in [3,5]:
        for data in ['int','data/urls','data/wiki']:
            lower = 8 if data=='int' else 10
            for exp in range(lower,16):
                for pl in range(8,100,8):
                    size = 2**exp
                    for config in 'dense1 hash hints'.split():
                        if config=='dense1' and data!='int':
                            continue
                        run(i,ycsb,data,2**exp,4096,config,pl)
