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

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

# parallel --joblog joblog-page-size --retries 50 -j 80% --memfree 16G  -- YCSB_VARIANT={3} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE={2} KEY_COUNT={1} DATA={4} ZIPF={6} {5} ::: 1e6 2e6 5e6 10e6 20e6 3e6 4e6 6e6 7e6 8e6 9e6 $(seq 11000000 1000000 19000000) ::: 0 1 8 512 ::: 3 4 5 ::: int data/urls data/wiki ::: page-size-builds/*/*-n3-ycsb ::: -1 0.99 > page-size.csv
r_par = read.csv('d1.csv', strip.white = TRUE) %>% mutate(
  sequential = FALSE
)
# with -j1, /sys/devices/system/cpu/cpufreq/boost = 0
r_seq = read.csv('d-sequential.csv', strip.white = TRUE) %>% mutate(
  sequential = TRUE
)
r = rbind(r_seq, r_par)
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
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
and sequential=TRUE
')

ggplot(d) +
  facet_nested(op ~ data_name, scales = 'free') +
  geom_bar(aes(config_name, scale / time), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si())

# lookup

## boxplot
ggplot(sqldf('select * from d where op="ycsb_c"')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time)) +
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

# insert
ggplot(sqldf('select * from d where op in ("ycsb_c_init")')) +
  facet_nested(. ~ data_name) +
  geom_boxplot(aes(config_name, scale / time)) +
  scale_y_continuous(labels = format_si(), expand = c(0, 0)) +
  expand_limits(y = 0)
