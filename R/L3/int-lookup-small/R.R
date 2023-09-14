source('../../common.R')

r<-rbind(
  # parallel -j4 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: 80000 90000 $(seq 100000 20000 1000000) $(seq 1100000 200000 10000000) 15000000 20000000 25000000 ::: named-build/hints-n3-ycsb named-build/dense2-n3-ycsb named-build/hash-n3-ycsb > int-lookup-small.csv
  # read.csv('d1-p.csv', strip.white = TRUE) %>% mutate(size=data_size * 12)
  # parallel -j1 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: 80000 90000 $(seq 100000 20000 1000000) $(seq 1100000 200000 10000000) 15000000 20000000 25000000 ::: named-build/hints-n3-ycsb named-build/dense2-n3-ycsb named-build/hash-n3-ycsb > int-lookup-small.csv
  read.csv('d1.csv', strip.white = TRUE) %>% mutate(size=data_size * ( (4096/4000) * (10+4+8)+32)),
  # parallel -j8 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e8 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: $(seq 30000000 5000000 70000000) ::: named-build/hints-n3-ycsb named-build/dense2-n3-ycsb named-build/hash-n3-ycsb > int-lookup-large.csv
  #read.csv('d1lp.csv', strip.white = TRUE) %>% mutate(size=data_size * ( (4096/4000) * (10+4+8)+32)),
  # parallel -j1 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e8 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: $(seq 30000000 5000000 70000000) ::: named-build/hints-n3-ycsb named-build/dense2-n3-ycsb named-build/hash-n3-ycsb > int-lookup-large.csv
  read.csv('d1l.csv', strip.white = TRUE) %>% mutate(size=data_size * ( (4096/4000) * (10+4+8)+32)),
  # parallel -j4 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e8 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: 80000 90000 $(seq 100000 20000 1000000) $(seq 1100000 200000 10000000) $(seq 15000000 5000000 70000000) ::: named-build/dense1-n3-ycsb > int-lookup-dense.csv
  #read.csv('d1d-p.csv', strip.white = TRUE) %>% mutate(size=data_size * ( (4096/4000) * (10+4+8)+32)),
  # parallel -j1 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e8 PAYLOAD_SIZE=8 ZIPF=-1 DATA=int KEY_COUNT={3} {4} ::: $(seq 1 1000) ::: 3 ::: 80000 90000 $(seq 100000 20000 1000000) $(seq 1100000 200000 10000000) $(seq 15000000 5000000 70000000) ::: named-build/dense1-n3-ycsb > int-lookup-dense.csv
  read.csv('d1d.csv', strip.white = TRUE) %>% mutate(size=data_size * ( (4096/4000) * (10+4+8)+32))
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
r <- sqldf('select * from r where op in ("ycsb_c") and config_name!="art"')

d<-r

axisRatio<-100
ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(data_size, cycle,col=config_name), stat = "summary", fun = mean) +
  geom_line(aes(data_size, LLC_miss*axisRatio,col=config_name),linetype='dotted', stat = "summary", fun = mean) +
  geom_smooth(aes(data_size, cycle,col=config_name), linetype='solid') +
  geom_smooth(aes(data_size, LLC_miss*axisRatio,col=config_name),linetype='dotted') +
  geom_line(aes(data_size, cycle,col=config_name),linetype='dashed', stat = "summary", fun = {function(x) length(x)*axisRatio}) +
  expand_limits(y = 0) +
  scale_y_continuous(sec.axis = sec_axis(~ . /axisRatio,'L3 miss'))+
  scale_x_log10(labels = format_si())

axisRatio<-100
ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(size, cycle,col=config_name), stat = "summary", fun = mean) +
  geom_line(aes(size, LLC_miss*axisRatio,col=config_name),linetype='dotted', stat = "summary", fun = mean) +
  geom_smooth(aes(size, cycle,col=config_name), linetype='solid') +
  #geom_smooth(aes(size, LLC_miss*axisRatio,col=config_name),linetype='dotted') +
  expand_limits(y = 0) +
  scale_y_continuous(sec.axis = sec_axis(~ . /axisRatio,'L3 miss'))+
  scale_x_log10(labels = label_number(scale_cut = cut_si("B")))

axisRatio<-100
ggplot(d %>% mutate(size=data_size * case_when(
  config_name == 'dense1' ~ 32+8.3,
  config_name == 'dense2' ~ 32+12.2,
  config_name == 'hash' ~ 32+1+6+12+0.2,
  config_name == 'hints' ~ 32+10+12+0.5,
  TRUE ~ NA
) )) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(size, cycle,col=config_name), stat = "summary", fun = mean) +
  geom_line(aes(size, LLC_miss*axisRatio,col=config_name),linetype='dotted', stat = "summary", fun = mean) +
  geom_smooth(aes(size, cycle,col=config_name), linetype='solid') +
  #geom_smooth(aes(size, LLC_miss*axisRatio,col=config_name),linetype='dotted') +
  expand_limits(y = 0) +
  scale_y_continuous(sec.axis = sec_axis(~ . /axisRatio,'L3 miss'))+
  scale_x_log10(labels = label_number(scale_cut = cut_si("B")))


ggplot() +
  scale_fill_hue()+
  geom_line(data=fetch2(d,'config_name','hints','hash','data_size','cycle'),mapping=aes(data_size, b-a),stat='summary',col='yellow') +
  geom_line(data=fetch2(d,'config_name','hints','hash','data_size','instr'),mapping=aes(data_size, b-a),stat='summary',col='white') +
  geom_line(data=fetch2(d,'config_name','hints','hash','data_size','LLC_miss'),mapping=aes(data_size, 10*(b-a)),stat='summary',col='red') +
  geom_line(data=fetch2(d,'config_name','hints','hash','data_size','L1_miss'),mapping=aes(data_size,  10*(b-a)),stat='summary',col='blue') +
  expand_limits(y = 0) +
  scale_y_continuous(sec.axis = sec_axis(~ . /axisRatio,'L3 miss'))+
  scale_x_log10(labels = format_si())

ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(data_size, LLC_miss,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels = format_si())

ggplot(
  d %>% mutate(l3a = case_when(
    config_name == 'dense1' ~ 2.5,
    config_name == 'dense2' ~ 3.5,
    config_name == 'hash' ~ 11,
    config_name == 'hints' ~ 8,
    TRUE ~ NA
  ))
) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_smooth(aes(data_size, LLC_miss - l3a,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels = format_si())

ggplot(
  d %>% mutate(a = case_when(
    config_name == 'dense2' ~ 0,
    config_name == 'dense1' ~ -350,
    config_name == 'hash' ~ 950,
    config_name == 'hints' ~ 500,
    TRUE ~ NA
  ))
) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_smooth(aes(data_size, cycle - a,col=config_name),stat="summary",fun=mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels = format_si())


# instr jump as tree height increases from 3 to 4 around 3.4e6 keys
ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(data_size, instr,col=config_name), stat = "summary", fun = mean) +
  scale_x_log10(labels = format_si())+
  expand_limits(y = 0)

# drop in L1 miss and branch miss at around 8e5 keys happens when root surpasses 32 entries and hints are used
ggplot(d) +
  scale_fill_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(data_size, br_miss,col=config_name), stat = "summary", fun = mean) +
  geom_line(aes(data_size, L1_miss,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_x_log10(labels = format_si())