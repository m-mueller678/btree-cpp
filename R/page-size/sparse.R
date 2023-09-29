source('../common.R')

# ./page-size-run.py > errors
r <- rbind(
  read.csv('sparse.csv', strip.white = TRUE),
  read.csv('sparse-large.csv', strip.white = TRUE)
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))

dwide <- rbind(
  d |>
    filter(psl == 12)|>
    mutate(kind = 'psi', x = psi),
  d |>
    filter(psi == 12)|>
    mutate(kind = 'psl', x = psl)
)

d|>
  filter(payload_size==64)|>
  group_by(config_name, data_name, op, psl, psi)|>
  count()|>
  View()

# main plot
dwide|>
  filter(!(config_name %in% c('prefix', 'inner')))|>
  mutate(y=scale/time)|>
  filter(config_name != 'dense1' & config_name!='dense2' | data_name =='int')|>
  #summarise(y=mean(y),.by=c(config_name,op,data_name,x,kind))|>
  #filter(kind=='psi')|>
  #filter(kind == 'psl')|>
  #filter(data_name=='int')|>
  filter(payload_size==8)|>
  ggplot() +
  facet_nested(op ~ config_name, scales = 'free_y') +
  geom_line(aes(x = x, y = y, col = data_name, linetype = kind), stat = 'summary', fun = mean) +
  scale_y_continuous(
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_x_continuous(labels = label_page_size) +
  scale_linetype_manual(
    values = c(psl = 'solid', psi = 'dashed'),
    labels = c(psl = 'leaf size', psi = 'inner size'),
    name = 'variable'
  ) +
  xlab('node size') +
  ylab('Throughout')+
  theme(
    legend.position = "bottom",
    axis.text.x = element_text(angle = 45, hjust = 1)
  )+
  expand_limits(y=0)

dwide|>
  #filter(!is.na(keys_per_leaf))|>
  mutate(y=scale/time)|>
  filter(kind == 'psl')|>
  #filter(data_name=='int' & config_name=='hash' & op=='ycsb_c' & kpl>400 & kpl<500)|>
  ggplot() +
  facet_nested(op ~ config_name, scales = 'free_y') +
  geom_line(aes(x = keys_per_leaf, y = y, col = data_name, linetype = factor(payload_size)), stat = 'summary', fun = mean) +
  scale_y_continuous(
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  expand_limits(y=0)+
  coord_cartesian(xlim=c(0,500))


# optimumn leaf sizes
d|>
  filter(psi == 12)|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(scale / time))|>
  group_by(data_name, op, config_name)|>
  slice_max(order_by = tp, n = 1, with_ties = FALSE)|>
  ggplot() +
  facet_nested(op ~ .) +
  geom_col(aes(x = config_name, y = psl, fill = data_name), position = 'dodge') +
  coord_cartesian(ylim = c(9, 15)) +
  scale_y_continuous(labels = label_page_size) +
  ylab('optimum leaf size') +
  xlab('configuration')

# throughput compared to best thoughput, heatmap
d|>
  filter(psi == 12 & payload_size==8)|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(scale / time))|>
  filter(!is.na(tp))|>
  group_by(data_name, op, config_name)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  group_by(op,config_name,data_name,psl)|>
  filter(!(config_name %in% c('baseline','prefix','heads','inner')))|>
  ggplot()+
  facet_nested(op~config_name)+
  geom_raster(aes(x=data_name,y=psl,fill=(tp/best_tp)))+
  scale_fill_viridis_c(limits = c(0.85,1.0),labels = scales::label_percent(),oob = scales::squish)+
  scale_fill_viridis_c(limits = c(0.85,1.0),labels = scales::label_percent(),oob = scales::squish)+
  scale_y_continuous(labels = label_page_size)+
  coord_cartesian(ylim=c(11,15))

# throughput compared to best thoughput, discrete
d|>
  filter(psi == 12 & payload_size==8)|>
  filter(config_name=='hints' & op=='ycsb_c')|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(scale / time))|>
  filter(!is.na(tp))|>
  group_by(data_name, op, config_name)|>
  mutate(best_tp = max(tp, na.rm = TRUE))|>
  group_by(op, config_name, data_name, psl)|>
  filter(!(config_name %in% c('baseline', 'prefix', 'heads', 'inner')))|>
  ggplot(aes(col = data_name, fill = data_name, x = psl, y = (tp / best_tp))) +
  facet_nested(op ~ config_name) +
  # geom_smooth(aes(col=data_name,x=psl,y=(tp/best_tp)),se=FALSE)+
  geom_rect(aes(fill = data_name, xmin = psl - 0.5, xmax = psl + 0.5, ymin = 0, ymax = (tp / best_tp)), alpha = 0.3, col = NA) +
  geom_segment(aes(col = data_name, x = psl - 0.5, xend = psl + 0.5, y = tp / best_tp, yend = tp / best_tp), size = 1) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_continuous(labels = label_page_size) +
  coord_cartesian(ylim = c(0.9, 1.0))+
  geom_vline(xintercept=12,linetype='dashed')

# throughput compared to best thoughput
d|>
  filter(psi == 12 & payload_size==8)|>
  #filter(config_name == 'hints' | config_name=='hash' | data_name =='int')|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(scale / time))|>
  filter(!is.na(tp))|>
  group_by(data_name, op, config_name)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  group_by(op,config_name,data_name,psl)|>
  #filter(config_name %in% c('baseline','prefix','heads','hints','inner'))|>
  #filter(config_name %in% c('hints','hash'))|>
  #filter(config_name %in% c('hints','dense1','dense2') & data_name=='int')|>
  ggplot(aes(col=data_name,fill=data_name,x=psl,y=(tp/best_tp)))+
  facet_nested(op~config_name)+
  geom_line(aes(col=data_name,x=psl,y=(tp/best_tp)))+
  scale_y_continuous(labels = label_percent())+
  scale_x_continuous(labels = label_page_size) +
  coord_cartesian(ylim=c(0.8,1.0))+
  geom_vline(xintercept=12,linetype='dashed')

# hint throughput compared to best thoughput
d|>
  filter(psi == 12 & payload_size==8)|>
  filter(config_name == 'hints')|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(scale / time))|>
  filter(!is.na(tp))|>
  group_by(data_name, op, config_name)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  group_by(op,config_name,data_name,psl)|>
  #filter(config_name %in% c('baseline','prefix','heads','hints','inner'))|>
  #filter(config_name %in% c('hints','hash'))|>
  #filter(config_name %in% c('hints','dense1','dense2') & data_name=='int')|>
  ggplot(aes(col=data_name,fill=data_name,x=psl,y=(tp/best_tp)))+
  facet_nested(op~config_name)+
  geom_line(aes(col=data_name,x=psl,y=(tp/best_tp)))+
  scale_y_continuous(labels = label_percent())+
  scale_x_continuous(labels = label_page_size) +
  coord_cartesian(ylim=c(0.8,1.0))+
  geom_vline(xintercept=12,linetype='dashed')

#hash
# throughput compared to best thoughput
d|>
  filter(psi == 12 & payload_size==8)|>
  #filter(config_name == 'hints' | config_name=='hash' | data_name =='int')|>
  mutate(kind = 'psl', x = psl)|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = mean(1/instr))|>
  filter(!is.na(tp))|>
  group_by(data_name, op, config_name)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  group_by(op,config_name,data_name,psl)|>
  filter(config_name %in% c('hints','hash'))|>
  ggplot(aes(col=data_name,fill=data_name,x=psl,y=(tp/best_tp)))+
  facet_nested(op~config_name)+
  geom_line(aes(col=data_name,x=psl,y=(tp/best_tp)))+
  scale_y_continuous(labels = label_percent())+
  scale_x_continuous(labels = label_page_size) +
  coord_cartesian(ylim=c(0.8,1.0))+
  geom_vline(xintercept=12,linetype='dashed')


