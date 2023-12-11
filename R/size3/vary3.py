def print_work(data, config, id, ycsb, psl, psi, sl=100, op=int(1e7), pl=8):
    path = (f'page-size-builds/_DPS_I_{psi}__DPS_L_{psl}/{config}-n3-ycsb')
    print(
        f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 {path}')


for run_id in range(5):
    for workload in [3, 5]:
        for d in [['int', 4411764], ['rng4', 4411764], ['data/urls-short', 2377103], ['data/wiki', 3466004]]:
            for var_size_b in range(8, 16):
                var_size = 2 ** var_size_b
                if var_size >= {'data/urls-short': 10, 'data/wiki': 9, 'int': 0, 'rng4': 0}[d[0]]:
                    for config in ["prefix", "heads", "hash", "hints", "dense2", "dense3"]:
                        if d[0] != 'int' and 'dense' in config:
                            continue
                        print_work(d, config, run_id, workload, var_size, 4096, pl=64)
                    if var_size!=4096:
                        print_work(d, 'hints', run_id, workload, 4096,var_size, pl=64)
# %%
