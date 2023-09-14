source('../common.R')

r = rbind(
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={4} {5} ::: $(seq 1 1000) ::: 3 5 :::  'DATA=data/urls KEY_COUNT=4268639' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' ::: $(seq 0.5 0.1 1.5) ::: named-build/hints-n3-ycsb > zipf300M.csv
  read.csv('d2.csv', strip.white = TRUE) %>% mutate(size=3e8),
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={4} {5} ::: $(seq 1 1000) ::: 3 5 :::  'DATA=data/urls KEY_COUNT=1422879' 'DATA=data/wiki KEY_COUNT=3272786' 'DATA=int KEY_COUNT=8333333' ::: $(seq 0.5 0.1 1.5) ::: named-build/hints-n3-ycsb > zipf100M.csv
  read.csv('d2_100M.csv', strip.white = TRUE) %>% mutate(size=1e8),
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={4} {5} ::: $(seq 1 1000) ::: 3 5 :::  'DATA=data/urls KEY_COUNT=142287' 'DATA=data/wiki KEY_COUNT=327278' 'DATA=int KEY_COUNT=833333' ::: $(seq 0.5 0.1 1.5) ::: named-build/hints-n3-ycsb > zipf10M.csv
  read.csv('d2_10M.csv', strip.white = TRUE) %>% mutate(size=1e7)
)

r$size=factor(r$size)


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
and op in ("ycsb_c")
')

ggplot(d) +
  scale_fill_hue()+
  facet_nested(size~., scales = 'free') +
  geom_line(aes(ycsb_zipf, scale/time, col=data_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


ggplot(d) +
  scale_fill_hue()+
  facet_nested(size~., scales = 'free') +
  geom_line(aes(ycsb_zipf, LLC_miss, col=data_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggplot(d %>% mutate(a= case_when(
  size == 3e8 ~ 27,
  size == 1e8 ~ 22,
  size == 1e7 ~ 13.5,
  TRUE ~ NA
) )) +
  scale_fill_hue()+
  facet_nested(config_name+data_name~op, scales = 'free') +
  geom_line(aes(ycsb_zipf, LLC_miss / a, col=size,linetype=size), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
