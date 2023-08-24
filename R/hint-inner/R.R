library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner', 'art')

r = read.csv('d1.csv', strip.white = TRUE)
r <- r %>% select(-starts_with("const"))
r$config_name = ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)

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
