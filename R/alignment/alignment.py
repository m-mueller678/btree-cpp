for run_id in range(100):
    for pl in [8,32, 256]:
        for d in [['int', 25_000_000], ['rng4', 25_000_000], ['data/urls-short', 4273260], ['data/wiki', 9818360]]:
            for bin in ['hints', 'hints-over-align']:
                print(f'env DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=6 SCAN_LENGTH=50 RUN_ID={run_id} OP_COUNT=1e8 PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 ./{bin}')
