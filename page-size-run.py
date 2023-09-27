#!/usr/bin/env python

import subprocess

def run(i,ycsb,data,leaf,inner,config):
    with open('page-size-large.csv','a') as f:
        env = {
            'RUN_ID':i,
            'YCSB_VARIANT':ycsb,
            'SCAN_LENGTH':100,
            'OP_COUNT':'1e7',
            'DATA': data,
            'KEY_COUNT': {'int':8823529,'data/urls':4751346,'data/wiki':6932008}[data],
            'PAYLOAD_SIZE':64,
            'ZIPF':-1,
        }
        env = {k:str(env[k]) for k in env}
        path=f'page-size-builds/-DPS_I={inner} -DPS_L={leaf}/{config}-n3-ycsb'
        result = subprocess.run(path, stdout=f, env=env)
        if result.returncode!=0:
            print(path,env,result)

for i in range(1000):
    for data in ['int','data/urls','data/wiki']:
        lower = 8 if data=='int' else 10
        for exp in range(lower,16):
            size = 2**exp
            for ycsb in [3,5]:
                for config in 'baseline dense1 dense2 hash heads hints inner prefix'.split():
                    run(i,ycsb,data,2**exp,4096,config)
                    run(i,ycsb,data,4096,2**exp,config)
