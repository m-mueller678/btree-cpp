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
r = read.csv('d1.csv', strip.white = TRUE)
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


ggplot(sqldf('
select data_name,config_name ,page_size,op,sum(scale/time)/count(*) as y from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
--and ycsb_zipf=0.99
--and data_size=10000000
--and payload_size=8
-- and data_name="int" and op="ycsb_c" and config_name="hints" and page_size=12
group by data_name,config_name ,page_size,op
having count(*)=160
')) +
  facet_nested(config_name ~ data_name, scales = 'free') +
  geom_line(aes(page_size, y, col = op)) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si())

ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c","ycsb_d","ycsb_e","ycsb_c_init")
--and ycsb_zipf=0.99
--and data_size=10000000
--and payload_size=8
--and data_name="int" and op="ycsb_c" and config_name="hints" and page_size=12
')) +
  facet_nested(config_name ~ data_name, scales = 'free', independent = "y") +
  geom_smooth(method = "loess", aes(page_size, scale / time, col = op)) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si())

ggplot(sqldf('
select * from r
where true
and op in ("ycsb_c_init")
and ycsb_zipf=0.99
and data_size=6000000
and payload_size=8
--and data_name="int" and op="ycsb_c" and config_name="hints" and page_size=12
')) +
  facet_nested(page_size ~ data_name) +
  geom_col(aes(config_name, scale / time)) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = format_si())
