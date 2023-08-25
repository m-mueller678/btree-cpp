library(ggplot2)
library(sqldf)

CONFIG_NAMES = c('baseline', 'prefix', 'heads', 'hints', 'hash', 'dense', 'inner')

r=read.csv('data.csv', strip.white=TRUE)
r$config_name = ordered(r$config_name, levels=CONFIG_NAMES, labels=CONFIG_NAMES)

ggplot(sqldf('
select * from r
where true
and op="lookup"
and payload_size=4
-- and data_size=1000000
and run_id=1
--and config_name="baseline"
--and data_name="file:data/urls"
--and (config_name in ("hints","hash","dense","inner"))
')) +
  facet_grid( data_name ~ config_name ) +
  geom_line(aes(data_size,scale/time))+
  #scale_y_log10()+
  expand_limits(y=0)+
  scale_x_log10()

ggplot(sqldf('
select * from r
where true
and op="lookup"
and payload_size=4
and run_id=1
--and config_name="baseline"
--and data_name="file:data/urls"
--and (config_name in ("hints","hash","dense","inner"))
')) +
  facet_grid( data_name ~ data_size ) +
  geom_col(aes(config_name,scale/time))+
  #scale_y_log10()+
  expand_limits(y=0)
  #scale_x_log10()


### hints vs inner

ggplot(sqldf('
select * from r
where true
and op="lookup"
and run_id=1
and config_name in ("hints","inner")
')) +
  facet_grid( data_name ~ payload_size ) +
  geom_line(aes(data_size,scale/time,col=config_name))+
  #scale_y_log10()+
  expand_limits(y=0)+
  scale_x_log10()

ggplot(sqldf('
select * from r
where true
and op="lookup"
and run_id=1
and config_name in ("hints","inner")
')) +
  facet_grid( data_name ~ payload_size ) +
  geom_line(aes(data_size,instr,col=config_name))+
  #scale_y_log10()+
  expand_limits(y=0)+
  scale_x_log10()