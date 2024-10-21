source('../common.R')

r <- bind_rows(
  read_broken_csv('skew3b.csv.gz'),
  read_broken_csv('lits.csv.gz'),
)

d<-r|>
  augment()|>
  filter(op=='ycsb_c')|>
  group_by(op,data_name,config_name,ycsb_zipf)|>
  arrange(run_id)|>
  slice_head(n=5)|>
  summarize(txs = median(scale/time))

data <- d|>
  pivot_wider(names_from = config_name, values_from = txs, values_fn = median,names_prefix = 'txs_')|>
  mutate(ra=txs_adapt2)|>
  pivot_longer(contains('txs_'),names_prefix = 'txs_')|>
  mutate(config_name=factor(name,levels=CONFIG_NAMES),value=value/ra)|>
  filter(config_name %in% c('art', 'hot','tlx','wh','lits'))

labels <- data|>
  group_by(data_name, config_name)|>
  filter(ycsb_zipf < 0.8)|>
  summarize(
    #up=config_name!='tlx',
    max = max(value),
    min = min(value),
    start = first(value,order_by = ycsb_zipf),
  )|>
  ungroup()|>
  arrange(data_name,start)|>
  mutate(
    above = c(FALSE,FALSE,TRUE,TRUE,TRUE,
              FALSE,FALSE,FALSE,TRUE,TRUE,
              FALSE,FALSE,FALSE,TRUE,TRUE,
              FALSE,FALSE,TRUE,TRUE,TRUE),
    data_name,
    #above = !(config_name=='tlx' | data_name %in% c('ints','sparse') & config_name=='hot'),
    y = ifelse(above , max, min),
    x = ifelse(config_name=='tlax',1,0.5),
    h = ifelse(config_name=='tlax',0.5,0),
    v = ifelse(above , -0.1, 1.1),
    c = CONFIG_LABELS[config_name],
    config_name,
    .keep = 'none'
  )|>
  ungroup()|>
  add_row(data_name = factor('data/wiki', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
  add_row(data_name = factor('data/urls-short', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
  add_row(data_name = factor('rng4', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
  add_row(data_name = factor('int', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))

colors <- c(
  brewer_pal(palette = 'RdBu')(8)[c(7, 8)],
  brewer_pal(palette = 'Dark2')(6)[c(2,4,6,5,1)]
)

data|>
  ggplot() +
  theme_bw() +
  facet_nested(. ~ data_name, scales = 'free', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
  )) +
  scale_y_log10(expand = expansion(mult = c(0, 0)),breaks = c(1/4,1/2,1,2,4),labels = c("1/4","1/2",1,2,4))+
  coord_cartesian(ylim= c(.25, 4))+
  geom_hline(yintercept = 1, col = brewer_pal(palette = 'RdBu')(8)[7],linewidth=0.25,linetype='dashed') +
  geom_line(mapping=aes(x = ycsb_zipf, y = value, col = config_name),linewidth=0.5,alpha=0.9) +
  geom_text(data = labels, mapping = aes(x = x, y = y, col = config_name, hjust = h, vjust = v, label = c), size = 2.5) +
  scale_color_manual(values = colors, breaks = factor(c('adapt2', 'art', 'hot', 'tlx','wh','lits'), levels = CONFIG_NAMES)) +
  #scale_fill_manual(values = colors) +
  scale_x_continuous(
    breaks = (1:3) * 0.5,
    name = 'Zipf-Parameter',
    expand = expansion(0),
  ) +
  expand_limits(y = c(0, 1.1)) +
  guides(fill = 'none', col = 'none') +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_text(size = 8,hjust=c(0,0.5,1)) ,
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8, hjust = 0.6),
    axis.title.x = element_text(size = 8),
    panel.spacing.x = unit(1, "mm"),
    #axis.ticks.x = element_blank(),
    plot.margin = margin(0, 8, 0, 0),
  ) +
  labs(y = 'Normalized lookup/s (log)')
save_as('zipf-in-mem', 35)


