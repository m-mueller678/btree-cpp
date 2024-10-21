source('../common.R')

COL_NAMES = c('tag','config_name','data_name','before','after','diff')

r<-bind_rows(
  read_csv("in-mem-size.csv.gz",col_names=COL_NAMES)|>filter(config_name!='lits'),
  read_csv("lits.csv.gz",col_names=COL_NAMES)
)

colors <- c(
  brewer_pal(palette = 'RdBu')(8)[c(6, 8)],
  brewer_pal(palette = 'Dark2')(6)[c(2,4,6,5,1)]
)

r|>
  mutate(
    data_name = factor(data_name, levels = names(DATA_MAP), labels = DATA_MAP),
    config_name=factor(config_name, levels = CONFIG_NAMES)
  )|>
  ggplot() +
  theme_bw() +
  facet_nested(. ~ data_name, scales = 'free', labeller = labeller(
    data_name = DATA_LABELS,
  )) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_text(angle = 90, hjust = 1, size = 7, vjust = 0.5), ,
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    panel.spacing.x = unit(0.5, "mm"),
    #axis.ticks.x = element_blank(),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(-10, 0, 0, 0),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
  ) +
  theme(axis.title.y = element_text(size = 8)) +
  scale_color_manual(values = colors) +
  scale_fill_manual(values = colors) +
  geom_point(aes(fill = config_name, col = config_name), x = 0, y = -1, size = 0) +
  labs(x = NULL, y = 'Size (GB)', fill = 'Worload', col = 'Workload') +
  guides(col = 'none', fill = 'none') +
  geom_col(aes(config_name, diff/1e9, fill = config_name)) +
  scale_y_continuous(breaks = (0:10)*0.5, expand = expansion(mult = c(0, 0.05))) +
  scale_x_manual(values = (1:8), labels = c('Base', 'Adapt', 'ART', 'HOT', 'TLX', 'WH','LITS')) +
  coord_cartesian(xlim = c(1, 7))

save_as('in-mem-size',35)