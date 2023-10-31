source('../common.R')

r <- bind_rows(
  #parallel -j16 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=-1 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee in-mem.csv
  read_broken_csv('../../in-mem.csv')
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(scale > 0)

d|>
    ggplot()+
  facet_nested(op ~ data_name,scales='free')+
  geom_bar(aes(x=config_name,y=scale/time,fill=config_name),stat='summary',fun=length)+
  scale_fill_hue()