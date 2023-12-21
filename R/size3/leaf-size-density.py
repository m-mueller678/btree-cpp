from random import shuffle


def print_work(data, config, id, ycsb, psl, psi, sl=100, op=int(1e7), pl=8,density=1.0):
    path = (f'page-size-builds/_DPS_I_{psi}__DPS_L_{psl}/{config}-n3-ycsb')
    print(
        f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY={density} {path}')


for run_id in range(10):
    d=['int', 4411764]
    for density_i in range(10,101):
        for var_size_b in range(8, 16):
            var_size = 2 ** var_size_b
            print_work(d, 'dense3', run_id, 3, var_size,4096,density=density_i/100,op=0)
# %%