def print_work(data, config, id, ycsb, psl, psi, sl=100, op=int(1e7), pl=8):
    path = (f'page-size-builds/_DPS_I_{psi}__DPS_L_{psl}/{config}-n3-ycsb')
    print(f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 {path}')


for run_id in range(3):
    for workload in [3, 5]:
        for config in ["baseline", "dense1", "dense2", "dense3", "hash", "heads", "hints", "inner", "prefix"]:
            for d in [['int', 25_000_000], ['rng4', 25_000_000], ['data/urls-short', 4273260], ['data/wiki', 9818360]]:
                for var_size_b in range(8, 16):
                    var_size = 2 ** var_size_b
                    if d[0] in ['int', 'rng4'] or var_size_b >= 10:
                        print_work(d, config, run_id, workload, 4096, var_size)
                        print_work(d, config, run_id, workload, var_size, 4096)
# %%
