for run_id in range(10):
    for scale in [1,0.25,0.5,0.125,2.0,4.0]:
        for config in ["prefix","heads", "hints", "art", "baseline", "dense1", "dense2", "dense3", "hash","inner", "hot", "tlx","adapt"]:
            for d in [['int',25_000_000],['rng4',25_000_000],['data/urls-short',4273260],['data/wiki',9818360]]:
                for zipf in range(50,151,4):
                    kc = int(d[1]*scale)
                    z=zipf*0.01
                    print(f'env DATA={d[0]} KEY_COUNT={kc} YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID={run_id} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={z} DENSITY=1 named-build/{config}-n3-ycsb')
