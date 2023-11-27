def print_work(data,config,id,ycsb,sl=100,op=int(1e7),pl=8):
    print(f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 named-build/{config}-n3-ycsb')

for partitions in sorted(list({int(100*1.1**i) for i in range(0,170)})):
    print_work(['partitioned_id',int(1e7)],'hints',1,402,sl=partitions,op=0)
#%%
