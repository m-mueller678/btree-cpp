for run_id in range(5):
    for config in ["baseline","adapt2","art", "hot", "tlx","wh"]:
        for d in [['int',25_000_000],['rng4',25_000_000],['data/urls-short',4273260],['data/wiki',9818360]]:
            for zipf in range(50,151,2):
                kc = int(d[1])
                z=zipf*0.01
                print(f'env LD_LIBRARY_PATH=. DATA={d[0]} KEY_COUNT={kc} YCSB_VARIANT=3 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={z} DENSITY=1 named-build/{config}-n3-ycsb')
