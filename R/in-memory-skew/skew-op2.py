for run_id in range(1):
    for config in ["prefix","heads", "hints", "art", "baseline", "dense1", "dense2", "dense3", "hash","inner", "hot", "tlx"]:
        for d in [['int',25_000_000],['rng4',25_000_000],['data/urls-short',4273260],['data/wiki',9818360]]:
            for zipf in range(50,151,2):
                kc = int(d[1])
                z=zipf*0.01
                print(f'env DATA={d[0]} KEY_COUNT={kc} YCSB_VARIANT=6 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=1e8 PAYLOAD_SIZE=8 ZIPF={z} DENSITY=1 named-build/{config}-n3-ycsb')
