source('../common.R')

# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={1} DENSITY=1 {4} ::: $(seq -w 50 1 150 | xargs -I{} echo "scale=2;{}/100" | bc) ::: 3 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hot-n3-ycsb named-build/art-n3-ycsb named-build/dense1-n3-ycsb named-build/hash-n3-ycsb | tee R/in-memory-skew/seq.csv
r<-read_broken_csv('seq.csv')

d <- r |>
  filter(op != "ycsb_e_init")|>
  augment()|>
  filter(scale > 0)

d|>
  pivot_longer(cols=c('txs','LLC_miss','L1_miss','br_miss','instr'),values_to = 'y',names_to='metric')|>
  filter(op=='ycsb_c')|>
  ggplot()+
  facet_nested(metric~data_name,scales = 'free_y')+
  geom_line(aes(y=y,x=ycsb_zipf,col=config_name))

d|>
  filter(op=='ycsb_c')|>
  ggplot()+
  facet_nested(.~data_name,scales = 'free_y')+
  geom_line(aes(y=txs,x=ycsb_zipf,col=config_name))

d|>
  filter(op=='ycsb_c',data_name %in% c('ints','sparse'))|>
  filter(ycsb_zipf>0.5)|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = OUTPUT_COLS, values_fn = mean)|>
  ggplot(aes(y=txs_art/txs_dense1,x=ycsb_zipf))+
  facet_nested(.~data_name,scales = 'free_y')+
  geom_point()+
  geom_smooth()+
  labs(y=NULL,x='Zipf Parameter')+
  theme_bw()

