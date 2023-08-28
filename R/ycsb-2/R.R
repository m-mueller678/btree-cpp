library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

# parallel --joblog joblog-full --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={1} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={2} KEY_COUNT={3} DATA={4} {5} ::: 3  4 5 ::: 0 1 8 16 128 512 ::: $(seq 1000000 1000000 20000000) ::: int data/* ::: named-build/*-n3-ycsb > full.csv
# payload_size, op, data_name, data_size, config_name
# zipf 1.5
# rr = read.csv('d1.csv', strip.white = TRUE)
# parallel --joblog joblog-full --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={3} KEY_COUNT={1} DATA={4} ZIPF={6} {5} :::  $(seq 1000000 2000000 10000000) ::: 3 4 ::: 0 1 8 512 ::: int data/urls data/wiki ::: named-build/*-n3-ycsb ::: -1 0.99 > full.csv
# old zipf generator
rr = read.csv('d2.csv', strip.white = TRUE)
rr <- rr %>%
  mutate(avgKeySize = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))


r <- rr %>% select(-starts_with("const"))
r = sqldf('select * from r where data_name!="data/genome"')
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
and ycsb_zipf=0.99
and payload_size=8
')) +
  facet_nested(data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, scale / time, col = config_name)) +
  scale_x_log10() +
  expand_limits(y = 0)


ggplot(sqldf('
select * from r
where true
and payload_size=8
and ycsb_zipf=-1
-- and config_name in ("hash","hints")
and op in ("ycsb_c","ycsb_d","ycsb_d_init")
')) +
  facet_nested(config_name ~ data_name, scales = "free_x") +
  geom_line(aes(data_size * (avgKeySize + payload_size), scale / time / 1e6, col = op)) +
  expand_limits(y = 0) +
  coord_cartesian(ylim = c(0.5, 2.5))

ggplot(sqldf('
select * from r
where true
and payload_size=0
and data_size=5e6
and op in ("ycsb_c","ycsb_d","ycsb_e")
')) +
  facet_nested(op ~ data_name, scales = "free_y") +
  geom_col(aes(config_name, scale / time)) +
  expand_limits(y = 0)

#hash

ggplot(sqldf('
select * from r
where true
and config_name in ("hash","hints")
and data_name in ("int","data/urls")
and payload_size=8
')) +
  facet_nested(ycsb_zipf + data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, scale / time, col = config_name)) +
  scale_x_log10() +
  expand_limits(y = 0)

hash_hint_rel <- r %>%
  filter(config_name %in% c('hints', 'hash')) %>%
  arrange(payload_size, op, data_name, data_size, config_name) %>%
  group_by(payload_size, op, data_name, data_size, ycsb_zipf) %>%
  mutate(hash_txs_rel = scale / time / lag(scale / time, default = NaN)) %>%
  mutate(hash_l1_rel = L1_miss / lag(L1_miss, default = NaN)) %>%
  mutate(hash_instr_rel = instr / lag(instr, default = NaN)) %>%
  filter(config_name == 'hash')

ggplot(sqldf('
select * from hash_hint_rel
where true
')) +
  facet_nested(ycsb_zipf + data_name ~ payload_size, scales = 'free_y') +
  geom_line(aes(data_size, hash_txs_rel, col = op)) +
  #geom_line(aes(data_size, hash_txs_rel, col = op),stat="summary",fun=length)+
  scale_x_log10() +
  scale_y_log10()
