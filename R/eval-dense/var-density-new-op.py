def print_work(data,config,id,ycsb,sl=50,op=int(5e6),pl=8,density=1.0):
    print(f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY={density} named-build/{config}-n3-ycsb')


for run_id in range(10):
    for config in ['dense3','dense2','hints','dense1']:
        for density in range(4,101,2):
            print_work(['int',int(25e6)],config,run_id,6,op=int(5e6),density=density/100)
