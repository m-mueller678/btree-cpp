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

ggplot(r) +
  scale_fill_hue()+
  facet_nested(op~data_name+size, scales = 'free') +
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

d<-r

baseline2prefix <- fetch2Relative(d, 'config_name', 'baseline', 'prefix', 'op,data_name', 'scale/time')
mean(baseline2prefix$r)
extremesBy('r',baseline2prefix)
sqldf('select data_name,avg(r) from baseline2prefix group by data_name')

prefix2heads <- fetch2Relative(d, 'config_name', 'prefix', 'heads', 'op,data_name', 'scale/time')
mean(prefix2heads$r)
mean((prefix2heads %>% filter(data_name == 'int')) $r)
extremesBy('r',prefix2heads)

heads2hints <- fetch2Relative(d, 'config_name', 'heads', 'hints', 'op,data_name', 'scale/time')
mean(heads2hints$r)
mean((heads2hints %>% filter(data_name == 'int')) $r)
mean((heads2hints %>% filter(data_name != 'int')) $r)
extremesBy('r',(heads2hints %>% filter(data_name == 'int')))
extremesBy('r',(heads2hints %>% filter(data_name != 'int')))

hints2hash <- fetch2Relative(d, 'config_name', 'hints', 'hash', 'op,data_name', 'scale/time')
mean(hints2hash$r)
mean((hints2hash %>% filter(data_name == 'int')) $r)
mean((hints2hash %>% filter(data_name != 'int')) $r)
extremesBy('r',(hints2hash %>% filter(data_name == 'int')))
extremesBy('r',(hints2hash %>% filter(data_name != 'int')))
ggplot(
  sqldf('select count(*),avg(r) as r,case when (data_name=="int") then "ints" else "other" end as data_name,op from hints2hash group by data_name=="int",op')
)+
  facet_wrap(.~data_name, strip.position = "bottom") +
  geom_col(aes(op,r))+
  scale_y_continuous(labels = percent_format())

hints2inner <- fetch2Relative(d, 'config_name', 'hints', 'inner', 'op,data_name', 'scale/time')
mean(hints2inner$r)
mean((hints2hash %>% filter(data_name == 'int')) $r)
sqldf('select data_name,avg(r) from hints2inner group by data_name')
mean((hints2hash %>% filter(data_name != 'int')) $r)
extremesBy('r',hints2inner)


hints2dense1 <- fetch2Relative(d, 'config_name', 'hints', 'dense1', 'op,data_name', 'scale/time')
mean(hints2dense1$r)
mean((hints2dense1 %>% filter(data_name == 'int')) $r)
mean((hints2dense1 %>% filter(data_name != 'int')) $r)
sqldf('select * from hints2dense1 where data_name="int" order by r')

hints2dense2 <- fetch2Relative(d, 'config_name', 'hints', 'dense2', 'op,data_name', 'scale/time')
mean(hints2dense2$r)
mean((hints2dense2 %>% filter(data_name == 'int')) $r)
mean((hints2dense2 %>% filter(data_name != 'int')) $r)
sqldf('select * from hints2dense2 where data_name="int" order by r')


ggplot(baseline2prefix)+
  facet_wrap(.~data_name, strip.position = "bottom") +
  geom_col(aes(op,r))+
  scale_y_continuous(labels = percent_format())