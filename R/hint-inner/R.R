library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

r = read.csv('d1.csv', strip.white = TRUE)
r <- r %>% select(-starts_with("const"))
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)

#parallel --joblog joblog1 --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={3} RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={4} KEY_COUNT={1} DATA=int {2} ::: $(seq 1000000 1000000 10000000) ::: page-size-builds/32768/hints-n3-ycsb page-size-builds/32768/inner-n3-ycsb ::: 3 4 ::: 250 500 1000 2000 3000 > large-hint-inner.csv
l=read.csv('large.csv', strip.white = TRUE)
l <- l %>% select(-starts_with("const"))
l$config_name = ordered(l$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, scale / time,col=config_name)) +
  scale_x_log10()+
  expand_limits(y = 0)

ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, L1_miss,col=config_name)) +
  scale_x_log10()+
  expand_limits(y = 0)

ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, instr,col=config_name)) +
  scale_x_log10()+
  expand_limits(y = 0)


ggplot(sqldf('
select * from l
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(data_name+payload_size ~ op, scales = 'free_y') +
  geom_line(aes(data_size, scale / time,col=config_name)) +
  scale_x_log10()