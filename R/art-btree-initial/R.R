library(ggplot2)
library(sqldf)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

r = read.csv('d2.csv', strip.white = TRUE)
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)

ggplot(sqldf('
select * from r
where true
and op="ycsb_c"
--and payload_size=8
and data_size=1000000
and run_id=1
--and config_name="hints"
--and data_name="data/urls"
')) +
  facet_grid(data_name ~ config_name) +
  geom_line(aes(payload_size, scale / time)) +
  #scale_y_log10()+
  expand_limits(y = 0) +
  scale_x_log10()

ggplot(sqldf('
select * from r
where true
--and op="ycsb_c"
and payload_size=8
and data_size=1000000
and run_id=1
--and config_name="hints"
--and data_name="data/urls"
')) +
  facet_grid(data_name ~ op) +
  geom_col(aes(config_name, scale / time)) +
  #scale_y_log10()+
  expand_limits(y = 0)
