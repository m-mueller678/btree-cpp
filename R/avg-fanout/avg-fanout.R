source('../common.R')

r <- bind_rows(
  read_broken_csv('avg-fanout.csv.gz'),
  read_broken_csv('avg-fanout-2.csv.gz'),
)

d<-r|>filter(op=='ycsb_c')|>
  augment()|>
  transmute(fan_out=(inner_count+leaf_count-1) / inner_count,config_name,data_name,data_size,payload_size,leaf_count)#|>group_by(data_name)|>filter(data_size<median(data_size))|>ungroup()

d|>
  ggplot()+
  facet_nested(data_name~config_name,scales = 'free',independent = 'x')+
  geom_point(aes(x=leaf_count,y=fan_out,col=factor(payload_size)))+
  #geom_point(aes(x=data_size,y=data_size/leaf_count,col=factor(payload_size)))+
  #geom_point(aes(x=data_size,y=leaf_count,col=factor(payload_size)))+
  scale_color_brewer(palette = 'Dark2')+
  scale_x_log10()



d|>group_by(config_name,data_name)|>summarize(avg=mean(fan_out),med=median(fan_out))|>
  pivot_longer(c('avg','med'))|>
  group_by(data_name,name)|>
  mutate(r = value / lag(value))|>
  ggplot(aes(x=config_name,y=value,fill=config_name))+
  facet_nested(name~data_name)+
  geom_col(position = 'dodge')+
  geom_text(aes(label=round((r-1)*100,2)))

