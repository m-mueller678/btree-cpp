def print_work(data,config,id,ycsb,sl=100,op=int(1e7),pl=8):
    print(f'env DATA={data[0]} KEY_COUNT={data[1]} YCSB_VARIANT={ycsb} SCAN_LENGTH={sl} RUN_ID={id} OP_COUNT={op} PAYLOAD_SIZE={pl} ZIPF=0.99 DENSITY=1 named-build/{config}-n3-ycsb')


for density in range(101):
    df=density/100
    for config in ['dense1','dense2','dense3','hints']:
        print_work(['int',int(25e6)],config,1,401,op=0)
