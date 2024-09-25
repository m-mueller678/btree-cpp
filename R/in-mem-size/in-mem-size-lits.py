for d in [['int', 25_000_000], ['rng4', 25_000_000], ['data/urls-short', 4273260], ['data/wiki', 9818360]]:
    for config in ['lits']:
        path = (f'named-build/{config}-n3-ycsb')
        command = f'env LD_LIBRARY_PATH=. MEM_SIZE=1 DATA={d[0]} KEY_COUNT={d[1]} YCSB_VARIANT=6 SCAN_LENGTH=50 RUN_ID=0 OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {path}'
        print(f'while ! {command} ; do :; done')
