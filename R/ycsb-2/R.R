library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)


format_si <- function(...) {
  # https://stackoverflow.com/a/21089837
  # Based on code by Ben Tupper
  # https://stat.ethz.ch/pipermail/r-help/2012-January/299804.html

  function(x) {
    limits <- c(1e-24, 1e-21, 1e-18, 1e-15, 1e-12,
                1e-9, 1e-6, 1e-3, 1e0, 1e3,
                1e6, 1e9, 1e12, 1e15, 1e18,
                1e21, 1e24)
    prefix <- c("y", "z", "a", "f", "p",
                "n", "µ", "m", " ", "k",
                "M", "G", "T", "P", "E",
                "Z", "Y")

    # Vector with array indices according to position in intervals
    i <- findInterval(abs(x), limits)

    # Set prefix to " " for very small values < 1e-24
    i <- ifelse(i == 0, which(limits == 1e0), i)

    paste(format(round(x / limits[i], 1),
                 trim = TRUE, scientific = FALSE, ...),
          prefix[i])
  }
}


CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art', 'dense1', 'dense2')

# parallel --joblog joblog-full --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={1} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={2} KEY_COUNT={3} DATA={4} {5} ::: 3  4 5 ::: 0 1 8 16 128 512 ::: $(seq 1000000 1000000 20000000) ::: int data/* ::: named-build/*-n3-ycsb > full.csv
# payload_size, op, data_name, data_size, config_name
# zipf 1.5
# rr = read.csv('d1.csv', strip.white = TRUE)
# parallel --joblog joblog-full --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={3} KEY_COUNT={1} DATA={4} ZIPF={6} {5} :::  $(seq 1000000 1000000 20000000) ::: 3 4 ::: 0 1 8 512 ::: int data/urls data/wiki ::: named-build/*-n3-ycsb ::: -1 0.99 > full.csv
# old zipf generator
#rr = read.csv('d2.csv', strip.white = TRUE)


# parallel --joblog joblog-full-seq --retries 50 -j 1 -- YCSB_VARIANT={4} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE={3} KEY_COUNT={2} DATA={5} ZIPF={6} {7} ::: $(seq 1 50) ::: $(seq 1000000 1000000 30000000) ::: 0 1 8 256 ::: 3 5 ::: int data/urls data/wiki ::: -1 0.99 ::: $(find named-build/ -name '*-n3-ycsb' | grep -v art) :::  > full-seq.csv
rr = rbind(
  read.csv('full-seq.csv', strip.white = TRUE),
  read.csv('full-seq-dense-broken.csv', strip.white = TRUE)
)
r = sqldf('select * from rr where RUN_ID==1')
r <- r %>%
  mutate(avg_key_size = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))


r <- r %>% select(-starts_with("const"))
r = sqldf('select * from r where data_name!="data/genome"')
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
and ycsb_zipf=0.99
and payload_size=8
and run_id=1
')) +
  facet_nested(data_name ~ op, scales = 'free') +
  geom_line(aes(data_size, scale / time, col = config_name)) +
  expand_limits(y = 0)


ggplot(na.omit(sqldf('
select * from r
where true
and payload_size=8
--and ycsb_zipf=-1
-- and config_name in ("hash","hints")
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
and run_id=1
-- and data_name="data/urls"
'))) +
  facet_nested(config_name ~ data_name + ycsb_zipf, scales = "free") +
  geom_line(aes(data_size * (avg_key_size + payload_size), scale / time, col = op)) +
  expand_limits(y = 0) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_x_continuous(labels = format_si()) +
  scale_y_continuous(labels = format_si()) +
  geom_vline(xintercept = 64e6, linetype = "dashed", color = "yellow")

ggplot(sqldf('
select * from r
where true
and payload_size=0
and data_size=5e6
--and config_name in ("hints","hash","dense","inner","art")
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
')) +
  facet_nested(op ~ data_name + ycsb_zipf, scales = "free_y", independent = "y") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = format_si()) +
  geom_bar(aes(config_name, scale / time), stat = "summary", fun = mean) +
  expand_limits(y = 0)

ggplot(sqldf('
select * from r
where true
and payload_size=8
and data_size=5e6
--and config_name in ("hints","hash","dense","inner","art")
and run_id=1
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
')) +
  facet_nested(op ~ data_name + ycsb_zipf, scales = "free_y", independent = "y") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = format_si()) +
  geom_col(aes(config_name, scale / time)) +
  expand_limits(y = 0)


#hash

ggplot(sqldf('
select * from r
where true
and config_name in ("hash","hints")
and data_name in ("int","data/urls")
and payload_size=8
and run_id=1
')) +
  facet_nested(ycsb_zipf + data_name ~ op, scales = 'free_y') +
  geom_line(aes(data_size, scale / time, col = config_name)) +
  scale_x_log10() +
  expand_limits(y = 0)

hash_hint_rel <- r %>%
  filter(config_name %in% c('hints', 'hash')) %>%
  filter(run_id %in% c(1)) %>%
  arrange(payload_size, op, data_name, data_size, config_name) %>%
  group_by(payload_size, op, data_name, data_size, ycsb_zipf) %>%
  mutate(hash_txs_rel = scale / time / lag(scale / time, default = NaN)) %>%
  mutate(hash_l1_rel = L1_miss / lag(L1_miss, default = NaN)) %>%
  mutate(hash_instr_rel = instr / lag(instr, default = NaN)) %>%
  filter(config_name == 'hash')

ggplot(sqldf('
select * from hash_hint_rel
where true
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
')) +
  facet_nested(data_name + ycsb_zipf ~ payload_size, scales = 'free_y') +
  geom_line(aes(data_size, hash_txs_rel, col = op)) +
  #geom_line(aes(data_size, hash_txs_rel, col = op),stat="summary",fun=length)+
  scale_x_log10() +
  scale_y_log10()
