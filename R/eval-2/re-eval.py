for run_id in range(50):
    for d in [['int', 25_000_000], ['rng4', 25_000_000], ['data/urls-short', 4273260], ['data/wiki', 9818360]]:
        for config in ["baseline", "prefix", "heads", "hints", "hash", "inner", "hot", "art", "tlx", "adapt2"]:
            path = (f'named-build/{config}-n3-ycsb')
            print(f'env DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=6 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {path}')
            if config in ['hash','hints','adapt2']:
                print(f'env DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=5 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {path}')
                print(f'env DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=501 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {path}')

