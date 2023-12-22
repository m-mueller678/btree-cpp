def print_work(data,config,id,ycsb,sl=50,op=int(5e6),pl=8):
    print(f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 named-build/{config}-n3-ycsb')


for run_id in range(50):
    for config in ['dense1','dense2','dense3','hints']:
            print_work(['int',25_000_000],config,run_id,6)
            if run_id<10:
                for d in [['rng4',25_000_000],['data/urls-short',4273260],['data/wiki',9818360]]:
                    print_work(d,config,run_id,6)

for run_id in range(10):
    for partitions in sorted(list({int(100*1.1**i) for i in range(0,170)})):
        for config in ['dense3','hints']:
            print_work(['partitioned_id',int(1e7)],config,run_id,402,sl=partitions,op=0)
#%%
