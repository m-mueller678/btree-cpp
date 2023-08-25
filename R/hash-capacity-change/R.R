library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')
HASH_NAMES = c('old', 'new', 'lazy', 'space', 'less')

#parallel -j80% --memfree 16G --retries -1 --joblog joblog -- KEY_COUNT={1} PAYLOAD_SIZE=8 DATA={2} OP_COUNT=5e6 RUN_ID={3} SCAN_LENGTH=0 YCSB_VARIANT=4 {4} ::: 1e6 2e6 3e6 4e6 5e6 ::: int data/* ::: $(seq 1 30) ::: ./hash-* > hash-cap.csv
# collected over multiple runs
rr = read.csv('d1.csv', strip.white = TRUE)
# collected in single run
#rr = read.csv('d2.csv', strip.white = TRUE)
rr <- rr %>%
  mutate(avgKeySize = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))
rr <- rr %>%
  mutate(bin_name = case_when(
    bin_name == './hash-bad-cap-b9f47e8a18377416f0a2774c84b1c83c519906c0' ~ 'old',
    bin_name == './hash-smart-cap-cdb9a778c94679727d5f5361866ebd212f06b7cb' ~ 'new',
    bin_name == './hash-lazy-d912146c3856a1eda466237cbb4980a3ff9e4c55' ~ 'lazy',
    bin_name == './hash-space-b2d292c1003c466a10bc99412af861b2948c4048' ~ 'space',
    bin_name == './hash-less-1cf34e22a073193d1e29b0e53d90b0d10a5188c6' ~ 'less',
    TRUE ~ bin_name
  ))

rr$config_name = ordered(rr$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)
rr <- rr %>% select(-starts_with("const"))
rr$bin_name = ordered(rr$bin_name, levels = HASH_NAMES, labels = HASH_NAMES)

r = sqldf('select * from rr where data_name!="data/genome"')


ggplot(sqldf('
select * from r
where true
')) +
  facet_nested(data_name + op ~ ., scales = 'free_y') +
  geom_smooth(aes(data_size, scale / time, col = bin_name, linetype = bin_name))
#geom_line(aes(data_size, scale / time, col = bin_name), stat = 'summary', fun = mean)

ggplot(sqldf('
select * from r
where true
')) +
  facet_nested(data_name + op ~ ., scales = 'free_y') +
  geom_smooth(aes(data_size, instr, col = bin_name, linetype = bin_name))
#geom_line(aes(data_size, instr, col = bin_name), stat = 'summary', fun = mean)
#geom_line(aes(data_size, instr, col = bin_name),stat='summary',fun=mean)

ggplot(sqldf('
select * from r
where true
')) +
  facet_nested(data_name + op ~ ., scales = 'free_y') +
  geom_smooth(aes(data_size, L1_miss, col = bin_name, linetype = bin_name))
#geom_line(aes(data_size, L1_miss, col = bin_name), stat = 'summary', fun = mean)
#geom_line(aes(data_size, L1_miss, col = bin_name),stat='summary',fun=mean)
