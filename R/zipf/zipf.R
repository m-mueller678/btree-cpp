source('../common.R')

#r = read.csv('d1-s.csv', strip.white = TRUE)
# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={4} {5} ::: $(seq 1 1000) ::: 3 5 ::: 'DATA=data/urls KEY_COUNT=4268639' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000'  ::: $(seq 0.5 0.01 1.5) ::: named-build/*-n3-ycsb > zipf.csv
r = read.csv('d1.csv', strip.white = TRUE)


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


d <- sqldf('
select * from r
where true
and config_name!="art"
and op in ("ycsb_c","ycsb_e")
')
d <- sqldf('select d.*,d2.* from d join d as d2 using (run_id,op,data_name,config_name) where d2.ycsb_zipf=0.5')
names(d)[(ncol(d) - ncol(d)/2 + 1):ncol(d)] <- paste("z0_", tail(names(d), ncol(d)/2), sep = "")

ggplot(d) +
  scale_fill_hue()+
  facet_nested(config_name~data_name, scales = 'free') +
  geom_line(aes(ycsb_zipf, scale / time,col=op), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(sqldf('select * from d where op="ycsb_c"')) +
  scale_fill_hue()+
  facet_nested(data_name~., scales = 'free') +
  geom_line(aes(ycsb_zipf, scale / time - z0_scale/z0_time,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(sqldf('select * from d where op="ycsb_e"')) +
  scale_fill_hue()+
  facet_nested(data_name~., scales = 'free') +
  geom_line(aes(ycsb_zipf, scale/time,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(sqldf('select * from d')) +
  scale_color_hue()+
  facet_nested(data_name~., scales = 'free') +
  geom_line(aes(ycsb_zipf, (scale / time) / (z0_scale/z0_time),col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_log10(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# TODO looks weird
ggplot(sqldf('select * from d')) +
  scale_color_hue()+
  facet_nested(data_name~., scales = 'free') +
  geom_line(aes(ycsb_zipf, instr/z0_instr,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_log10(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(sqldf('select * from d')) +
  scale_color_hue()+
  facet_nested(data_name~., scales = 'free') +
  geom_line(aes(ycsb_zipf, LLC_miss/z0_LLC_miss,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_log10(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))