library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

# parallel --joblog joblog-full --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={1} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={2} KEY_COUNT={3} DATA={4} {5} ::: 3  4 5 ::: 0 1 8 16 128 512 ::: $(seq 1000000 1000000 20000000) ::: int data/* ::: named-build/*-n3-ycsb > full.csv
# payload_size, op, data_name, data_size, config_name
rr = read.csv('d1.csv', strip.white = TRUE)
r <- rr %>% select(-starts_with("const"))
r=sqldf('select * from r where data_name!="data/genome"')
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(payload_size ~ op+data_name, scales = 'free_y') +
  geom_line(aes(data_size, scale / time,col=config_name)) +
  scale_x_log10()+
  expand_limits(y = 0)

ggplot(sqldf('
select * from r
where true
and payload_size=8
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(config_name ~ data_name) +
  geom_line(aes(data_size, scale / time,col=op)) +
  scale_x_log10()+
  expand_limits(y = 0)+
  ylim(0,3e7)

ggplot(sqldf('
select * from r
where true
and payload_size=0
and data_size=5e6
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(op ~ data_name,scales="free_y") +
  geom_col(aes(config_name,scale / time)) +
  expand_limits(y = 0)
