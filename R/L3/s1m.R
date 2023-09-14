source('../common.R')

r<-rbind(
  # parallel -j8 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=5e5 PAYLOAD_SIZE=8 ZIPF=-1 {4} ::: $(seq 1 10) ::: 3 5 ::: 'DATA=data/urls KEY_COUNT=14228' 'DATA=data/wiki KEY_COUNT=32727' 'DATA=int KEY_COUNT=83333' ::: named-build/*-n3-ycsb > s1m-p.csv
  #read.csv('s1m.csv', strip.white = TRUE) %>% mutate(size=1e6),
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=5e5 PAYLOAD_SIZE=8 ZIPF=-1 {4} ::: $(seq 1 1000) ::: 3 5 ::: 'DATA=data/urls KEY_COUNT=14228' 'DATA=data/wiki KEY_COUNT=32727' 'DATA=int KEY_COUNT=83333' ::: named-build/*-n3-ycsb > s1m.csv
  read.csv('s1m2.csv', strip.white = TRUE) %>% mutate(size=1e6),
  read.csv('../mod-cmp/d2.csv', strip.white = TRUE) %>% mutate(size=3e8)
)


r <- r %>%
  mutate(avg_key_size = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))

r$page_size = log(r$const_pageSize, 2)
r <- r %>% select(-starts_with("const"))
r = sqldf('select * from r where data_name!="data/genome"')
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)
r <- sqldf('select * from r where op in ("ycsb_c","ycsb_e","ycsb_c_init") and config_name!="art"')

d<-r
dintl <- sqldf('select * from d where data_name="int" and op="ycsb_c"')
durls <- sqldf('select * from d where data_name="data/urls" and op="ycsb_e"')
dints <- sqldf('select * from d where data_name="int" and op="ycsb_e"')

ggplot(r) +
  scale_fill_hue()+
  facet_nested(op+size~data_name, scales = 'free') +
  geom_bar(aes(config_name, scale / time,fill=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(r) +
  scale_fill_hue()+
  facet_nested(op~data_name+size, scales = 'free') +
  geom_bar(aes(config_name, scale,fill=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

sizeThrouhput = fetch2(d,'size',3e8,1e6,'data_name,op,config_name','scale/time')
ggplot(sizeThrouhput) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  scale_y_log10(breaks=seq(1,10))+
  geom_bar(aes(config_name, b/a,fill=config_name), stat = "summary", fun = mean) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(dintl) +
  scale_fill_hue()+
  facet_nested(op~size) +
  geom_bar(aes(config_name, scale/time,fill=config_name), stat = "summary", fun = mean) +
  scale_y_log10() +
  coord_cartesian(ylim = c(1e6,3e7) ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(dintl) +
  scale_fill_hue()+
  facet_nested(op~size) +
  geom_bar(aes(config_name, LLC_miss,fill=config_name), stat = "summary", fun = mean) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+
  scale_y_continuous(breaks = 5*seq(0,100))


ggplot(dintl) +
  scale_fill_hue()+
  facet_nested(op~size) +
  scale_y_log10() +
  geom_bar(aes(config_name, LLC_miss,fill=config_name), stat = "summary", fun = mean) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


ggplot(dintl) +
  scale_fill_hue()+
  facet_nested(op~size) +
  geom_bar(aes(config_name, br_miss,fill=config_name), stat = "summary", fun = mean) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(dintl) +
  scale_fill_hue()+
  facet_nested(op~size) +
  geom_bar(aes(config_name, scale/time,fill=config_name), stat = "summary", fun = mean) +
  scale_y_log10() +
  coord_cartesian(ylim = c(1e6,3e7) ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

fetch2Relative(dintl, 'config_name', 'hints', 'hash', 'op,data_name,size', 'scale/time')
fetch2Relative(dintl, 'config_name', 'hints', 'dense2', 'op,data_name,size', 'scale/time')
fetch2Relative(dintl, 'config_name', 'hints', 'dense2', 'op,data_name,size', 'LLC_miss')
fetch2Relative(dintl, 'config_name', 'hints', 'dense1', 'op,data_name,size', 'scale/time')

fetch2Relative(durls, 'config_name', 'baseline', 'prefix', 'op,data_name,size', 'scale/time')
fetch2Relative(dints, 'config_name', 'hints', 'dense1', 'op,data_name,size', 'scale/time')