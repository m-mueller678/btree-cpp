import random

while True:
    scale = random.random()
    for pl_size in [6,8,12]:
        for d in [['int', 25_000_000], ['rng4', 25_000_000], ['data/urls-short', 4273260], ['data/wiki', 9818360]]:
            for config in ["baseline", "prefix", "heads", "hints"]:
                path = (f'named-build/{config}-n3-ycsb')
                size = d[1] / 16**scale
                print(f'env LD_LIBRARY_PATH=. DATA={d[0]} KEY_COUNT={size} YCSB_VARIANT=6 SCAN_LENGTH=50 OP_COUNT=1 PAYLOAD_SIZE={pl_size} ZIPF=0.99 DENSITY=1 {path}')

