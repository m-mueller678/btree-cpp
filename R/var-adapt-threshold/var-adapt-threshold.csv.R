source('../common.R')

# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: var-adapt/* | tee R/var-adapt-threshold/seq.csv
r<-read_broken_csv('seq.csv')

r|>ggplot()+
  facet_nested(data_name~op)+
  geom_point(aes(const_adapt_threshold,scale/time))

r|>group_by(op,data_name,const_adapt_threshold)|>summarize(txs=mean(scale/time),.groups = 'drop_last')|>summarize(r=max(txs)/min(txs),.groups = 'drop')

r|>group_by(const_adapt_threshold)|>summarize(txs=mean(scale/time),.groups = 'drop_last')|>summarize(r=max(txs)/min(txs)-1,.groups = 'drop')
