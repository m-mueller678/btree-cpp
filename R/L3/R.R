source('../common.R')

r<-rbind(
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=-1 KEY_COUNT={3} DATA={5} {4} ::: $(seq 1 100) ::: 3 ::: $(seq 100 100 900) $(seq 1000 1000 9000) $(seq 10000 10000 100000) $(seq 200000 100000 500000) ::: named-build/*-n3-ycsb ::: int data/urls data/wiki > l3.csv
  read.csv('d1.csv', strip.white = TRUE) %>% mutate(sequential = TRUE),
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=5e6 PAYLOAD_SIZE=8 ZIPF=-1 KEY_COUNT={3} DATA={5} {4} ::: $(seq 1 100) ::: 3 ::: $(seq 600000 100000 2000000) ::: named-build/*-n3-ycsb ::: int data/urls data/wiki > l3l.csv
  read.csv('d2.csv', strip.white = TRUE) %>% mutate(sequential = TRUE)
)



r <- r %>%
  mutate(avgKeySize = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))

r <- r %>% mutate(trueSize = data_size * (payload_size+avgKeySize))
r <- r %>% mutate(fullSize = trueSize + data_size * (24+avgKeySize))

r$page_size = log(r$const_pageSize, 2)
r <- r %>% select(-starts_with("const"))
r = sqldf('select * from r where data_name!="data/genome"')
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)

d <- sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_e","ycsb_c_init")
and config_name!="art"
and op="ycsb_c"
')


ggplot(d) +
  scale_color_hue()+
  facet_nested(data_name~.) +
  geom_line(aes( fullSize, LLC_miss,col=config_name,linetype=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  #scale_x_log10(labels=label_number_si())+
  coord_cartesian(xlim= c(1e5,2e6),ylim=c(0,20))+
  scale_y_continuous(labels = label_number_si(), expand = c(0, 1)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


ggplot(d) +
  scale_color_hue()+
  facet_nested(data_name~.) +
  geom_line(aes( trueSize, L1_miss,col=config_name,linetype=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels=label_number_si())+
  coord_cartesian(xlim= c(1e3,1e5),ylim=c(0,30))+
  scale_y_continuous(labels = label_number_si(), expand = c(0, 1)) +
  geom_vline( xintercept = 32000000,col='green')+
  geom_vline( xintercept = 1000000,col='green')+
  geom_vline( xintercept = 32000,col='green')+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

axisRatio <- 10
ggplot(d) +
  scale_color_hue()+
  facet_nested(data_name~.) +
  geom_line(aes( fullSize, time,col=config_name),linetype='solid', stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels=label_number_si())+
  scale_y_continuous(labels = label_number_si(), expand = c(0, 1),name="time [s]") +
  scale_y_continuous(labels = label_number_si(), expand = c(0, 1),sec.axis = sec_axis(~.*axisRatio,name="LLC misses")) +
  geom_vline( aes(xintercept = 550000*(avgKeySize+avgKeySize+payload_size+24) ))+
  geom_vline( xintercept = 32000000,col='green')+
  geom_vline( xintercept = 1000000,col='green')+
  geom_vline( xintercept = 32000,col='green')+
  geom_line(aes( fullSize, LLC_miss/axisRatio,col=config_name),linetype='dotted', stat = "summary", fun = mean)


ggplot(sqldf('select * from d where trueSize>=60000 and trueSize<=65000')) +
  scale_color_hue()+
  facet_nested(data_name~.) +
  geom_point(aes( trueSize, LLC_miss,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = label_number_si(), expand = c(0, 1)) +
  #expand_limits(x=0)+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

