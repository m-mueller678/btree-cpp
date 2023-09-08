source('../common.R')

#r_par = read.csv('d1.csv', strip.white = TRUE) %>% mutate(sequential = FALSE)
# with -j1, /sys/devices/system/cpu/cpufreq/boost = 0
# r_seq = read.csv('d-sequential.csv', strip.white = TRUE) %>% mutate(sequential = TRUE)
#r = rbind(r_seq, r_par)
# with  /sys/devices/system/cpu/cpufreq/boost = 0
# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=-1 {4} ::: $(seq 1 10) ::: 3 5 :::  'DATA=data/urls KEY_COUNT=4268639' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee mod-cmp.csv
r = read.csv('d2.csv', strip.white = TRUE) %>% mutate(sequential = TRUE)


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

# config_name 7
# op 4
# data_name 3
# total 84
d <- sqldf('
select * from r
where true
and config_name!="art"
and op in ("ycsb_c","ycsb_e","ycsb_c_init")
and sequential=TRUE
')


ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_bar(aes(config_name, scale / time,fill=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si(), expand = c(0, 1e5)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


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

hints2dense2 <- fetch2Relative(d, 'config_name', 'hints', 'dense2', 'op,data_name', 'scale/time')
mean(hints2dense2$r)
mean((hints2dense2 %>% filter(data_name == 'int')) $r)
mean((hints2dense2 %>% filter(data_name != 'int')) $r)



ggplot(
  hints2dense2
)+
  facet_wrap(.~data_name, strip.position = "bottom") +
  geom_col(aes(op,r))+
  scale_y_continuous(labels = percent_format())



base="baseline"
cmp="prefix"
metric='scale/time'
fn$sqldf(sprintf('select * from (select op,data_name,config_name,x/lag(x,1) over (partition by op,data_name order by config_name=="$cmp") as x2
 from (select config_name,op,data_name,avg($metric) as x from d group by op,data_name,config_name) where config_name in ("$base","$cmp") order by op,data_name,config_name) where config_name="$cmp"'))

# lookup

## boxplot
ggplot(sqldf('select * from d where op="ycsb_c"')) +
  facet_nested(. ~ data_name) +
  geom_violin(aes(config_name, scale / time)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

ggplot(sqldf('select * from d where op="ycsb_c" and config_name in ("baseline","prefix")')) +
  facet_nested(. ~ data_name) +
  geom_density(aes(time / scale, col = config_name)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

## hints
ggplot(sqldf('select * from d where op="ycsb_c" and config_name in ("heads","hints")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

ggplot(sqldf('select * from d where op="ycsb_c" and config_name in ("heads","hints")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, L1_miss)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

ggplot(sqldf('select * from d where op="ycsb_c" and config_name in ("heads","hints")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, instr)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

#prefix
ggplot(sqldf('select * from d where op="ycsb_c" and config_name in ("baseline","prefix")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, time / scale))

# insert
ggplot(sqldf('select * from d where op in ("ycsb_c_init","ycsb_d")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time, col = op)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

ggplot(sqldf('select * from d where op in ("ycsb_c_init") and config_name in ("heads","hints","dense")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)

# scan
ggplot(sqldf('select * from d where op in ("ycsb_e")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)
