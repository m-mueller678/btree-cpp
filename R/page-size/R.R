library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

# parallel --joblog joblog --retries -1 -j 80% --memfree 16G  -- YCSB_VARIANT={4} RUN_ID=1 OP_COUNT=5e4 PAYLOAD_SIZE={1} KEY_COUNT=3e4 DATA={2} {3} ::: 0 8 ::: int data/urls data/wiki ::: $(find page-size-builds/ | grep n3) ::: 3 4 5 > var-page-size.csv
# r = read.csv('d1.csv', strip.white = TRUE)
# parallel --joblog joblog --retries -1 -j 80% --memfree 16G  -- YCSB_VARIANT={4} RUN_ID=1 OP_COUNT=5e6 PAYLOAD_SIZE={1} KEY_COUNT=3e6 DATA={2} {3} ::: 0 8 ::: int data/urls data/wiki ::: $(find page-size-builds/ | grep n3) ::: 3 4 5 > var-page-size.csv
# r = read.csv('d2.csv', strip.white = TRUE)
# parallel --joblog joblog --retries -1 -j 80% --memfree 16G  -- YCSB_VARIANT={4} RUN_ID=1 OP_COUNT=3e7 PAYLOAD_SIZE={1} KEY_COUNT=5e6 DATA={2} {3} ::: 0 8 ::: int data/urls data/wiki ::: $(find page-size-builds/ | grep n3) ::: 3 4 5 > var-page-size.csv
# incomplete, not enough urls for insert
# r = read.csv('d3.csv', strip.white = TRUE)
# parallel --joblog joblog --retries -1 -j 80% --memfree 16G  -- YCSB_VARIANT={4} RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={1} KEY_COUNT=3e6 DATA={2} {3} ::: 0 8 ::: int data/urls data/wiki ::: $(find page-size-builds/ | grep n3) ::: 3 4 5 > var-page-size.csv
r = read.csv('d4.csv', strip.white = TRUE)

r$pageSize = log(r$const_pageSize, 2)
r <- r %>% select(-starts_with("const"))
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
--and data_name in ("data/wiki","int")
and payload_size=0
')) +
  facet_nested(config_name ~ data_name) +
  geom_line(aes(pageSize, scale / time, col = op))
  #+ylim(0, 2.0e7)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
--and data_name in ("data/wiki","int")
and payload_size=0
')) +
  facet_nested(config_name ~ data_name) +
  geom_line(aes(pageSize, L1_miss, col = op))


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e")
--and data_name in ("data/wiki","int")
and payload_size=0
')) +
  facet_nested(config_name ~ data_name) +
  geom_line(aes(pageSize, instr, col = op))

ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c_init","ycsb_d_init","ycsb_e_init")
--and data_name in ("data/wiki","int")
and payload_size=0
')) +
  facet_nested(config_name ~ data_name, scales = "free_y") +
  geom_line(aes(pageSize, instr, col = op))
