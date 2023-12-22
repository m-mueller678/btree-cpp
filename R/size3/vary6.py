avg_key_size = {'data/urls': 62.280, 'rng4': 4, 'data/urls-short': 62.204, 'data/wiki': 22.555, 'int': 4}

def print_work(data, config, id, ycsb, psl, psi, sl=50, op=int(5e6), pl=8):
    path = (f'page-size-builds/_DPS_I_{psi}__DPS_L_{psl}/{config}-n3-ycsb')
    print(
        f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 {path}')

for vary_inner in [False, True]:
    for run_id in range(10):
        for pl in [8,16,64]:
            for d in ['int', 'rng4', 'data/urls-short', 'data/wiki']:
                key_count = int(3e8/(avg_key_size[d] + pl))
                for var_size_b in range(8, 16):
                    var_size = 2 ** var_size_b
                    if vary_inner and var_size == 4096:
                        continue
                    if var_size >= {'data/urls-short': 10, 'data/wiki': 9, 'int': 0, 'rng4': 0}[d]:
                        for config in ["baseline", "prefix", "heads", "hints", "hash", "dense2", "dense3"]:
                            print_work([d,key_count], config, run_id, 6, 4096 if vary_inner else var_size,var_size if vary_inner else 4096,pl=pl)
# %%
