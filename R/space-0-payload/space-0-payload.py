for run_id in range(50):
    for d in [['data/urls-short',4822840],['data/wiki',13300820],['int',75000000],['rng4',75000000]]:
        for config in ["baseline", "prefix", "heads", "hints", "hash"]:
            path = (f'named-build/{config}-n3-ycsb')
            print(f'env DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=6 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=0 PAYLOAD_SIZE=0 ZIPF=0.99 DENSITY=1 {path}')