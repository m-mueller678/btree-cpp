source("../common.R")

# python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee R/eval-dense/var-density.csv
r_var_density <- bind_rows(
  # python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee -a R/eval-dense/var-density.csv
  #read_broken_csv('var-density.csv.gz'),
  #read_broken_csv('var-density-2.csv'),
  # python3 R/eval-dense/var-density-e.py |parallel -j1 --joblog joblog -- {1}| tee -a R/eval-dense/var-density-e.csv
  #read_broken_csv('var-density-e.csv.gz'),

  #christmas run
  read_broken_csv('var-density-new-op.csv.gz')
)

d_var_density <- r_var_density|>filter(run_id<5)|>augment()

d_config_pivot<-d_var_density|>pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = median)

d_var_density|>group_by(op,density,config_name)|>count()|>filter(n!=10)

grouped<-d_var_density|>group_by(across(!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))))|>
  summarize(
    txs=median(txs),
    node_count=median(node_count)
  )

dense_joined <- grouped|>
  filter(config_name %in% c('dense1', 'dense2', 'dense3'))|>
  full_join(grouped|>filter(config_name == 'hints'), by = c('op', 'data_name','density'), relationship = 'many-to-one')

{
  common <- function(op_filter, has_x) d_var_density|>
    filter(op==op_filter)|>
    ggplot() +
    theme_bw() +
    scale_color_brewer(palette = 'Dark2', name = "Configuration", labels = CONFIG_LABELS) +
    scale_x_continuous(
      name = if (has_x) { 'density' }else { NULL },
      limits = c(0, 1),
      breaks = (1:3)*0.25,
      labels = label_percent(),
      expand = expansion(add = 0)
    ) +
    (if (!has_x){theme(axis.ticks.x = element_blank(),axis.text.x=element_blank())}else{theme()})+
    guides(col = guide_legend(override.aes = list(size = 1), nrow = 2,title.position="top", title.hjust = 0.5))+
    expand_limits(y=0)


  txc <- common('ycsb_c',FALSE) +
    facet_nested(. ~ op, labeller = labeller(
      op = OP_LABELS,
    ),scales='free_y',independent = 'y') +
    geom_line(aes(x = density, y = txs, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))+
    facet_nested(. ~ 'ycsb-c')
  txi <- common('insert90',FALSE) +
    facet_nested(. ~ op, labeller = labeller(
      op = OP_LABELS,
    ),scales='free_y',independent = 'y') +
    geom_line(aes(x = density, y = txs, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')),position = 'right')+
    facet_nested(. ~ 'insert')
  txs <- common('scan',FALSE) +
    facet_nested(. ~ op, labeller = labeller(
      op = OP_LABELS,
    ),scales='free_y',independent = 'y') +
    geom_line(aes(x = density, y = txs, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')),position = 'right')+
    facet_nested(. ~ 'insert')
  space <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_bytes()) +
    facet_nested(. ~ 'space per record')
  dense <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = (nodeCount_Dense+nodeCount_Dense2) / leaf_count, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_percent(),position = 'right') +
    facet_nested(. ~ 'dense leaf share')
  (txc+txi+txs+space) + plot_layout(guides = "collect") &
    theme(legend.position = 'bottom',legend.title = element_blank(),plot.margin = margin(1),legend.margin = margin(-15,0,0,0))
}
save_as('var-density', 70)

# speedup, config facet
dense_joined|>
  filter(config_name.x %in% c('dense2','dense3'))|>
  ggplot()+theme_bw()+
  facet_nested(. ~ config_name.x, labeller = labeller('config_name.x' = CONFIG_LABELS)) +
  geom_line(aes(density,txs.x/txs.y-1,col=op))+
  scale_colour_brewer(palette = 'Dark2', labels = OP_LABELS)+
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = 0.05), breaks = (0:20) * 0.5)+
  scale_x_continuous(labels = label_percent(), expand = expansion(mult = 0), breaks = (0:3) * 0.25)+
  geom_hline(yintercept = 0) +
  theme(
    legend.position = 'right',
    legend.title = element_blank(),
    legend.margin = margin(-20,0,0,0),
  )
save_as('var-density', 20)

# speedup, op facet



dense_joined|>
  filter(config_name.x!='dense1')|>
  filter(op!='ycsb_e_init')|>
  ggplot() +
  theme_bw() +
  scale_color_brewer(palette = 'Dark2', name = NULL, labels = CONFIG_LABELS) +
  scale_x_continuous(
    name='Density (%)',
    limits = c(0, 1),
    breaks = (0:2)*0.5,
    labels = label_percent(suffix = ''),
    expand = expansion(add = 0)
  ) +
  scale_y_continuous(labels = label_percent(suffix = ''), expand = expansion(mult = c(.05, .05)),name='op/s Increase (%)') +
  guides(col = 'none')+
  expand_limits(y=0)+
  facet_nested(~op,scales = 'free_y',independent = 'y', labeller = labeller(
    op = OP_LABELS,
  ))+
  geom_line(aes(density,txs.x/txs.y-1,col=config_name.x))+
  geom_text(aes(x,y,col=config_name,label=c('dense2'='semi d.','dense3'='fully d.')[as.character(config_name)]),
            data = data.frame(
              op=factor(c('ycsb_c','insert90','scan'),levels=names(OP_LABELS)),
              config_name=factor(c(rep('dense3',3),rep('dense2',3)),levels=names(CONFIG_LABELS)),
              x=c(0.75,0.85,0.8,0.6,0.65,1),
              y=c(0.55,1.7,0.8,0.35,0.9,0.18)
            ),
            vjust='bottom',hjust='right',size=3
  )+
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    #axis.text.x = element_blank(),
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8,hjust=0.7),
    axis.title.x = element_text(size = 8),
    panel.spacing.x = unit(0.5, "mm"),
    #axis.ticks.x = element_blank(),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(-10, 0, 0, 0),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 10, 0, 2),
  )
save_as('var-density-txs', 25)

# space
{
  common <- function(op_filter, has_x) d_var_density|>
    filter(op==op_filter)|>
    ggplot() +
    theme_bw() +
    scale_color_brewer(palette = 'Dark2', name = "Configuration", labels = CONFIG_LABELS) +
    scale_x_continuous(
      name = NULL,
      limits = c(0, 1),
      breaks = (0:1)*0.5,
      labels = label_percent(),
      expand = expansion(add = 0)
    ) +
    (if (!has_x){theme(axis.ticks.x = element_blank(),axis.text.x=element_blank())}else{theme()})+
    guides(col = guide_legend(override.aes = list(size = 1), nrow=4,title.position="top", title.hjust = 0.5))+
    expand_limits(y=0)
  space <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_bytes())
  dense <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = (nodeCount_Dense+nodeCount_Dense2) / leaf_count, col = config_name)) +
    scale_y_continuous(name = NULL, labels = label_percent(),breaks=(0:4)*0.25,position = 'right')
  (space|dense) + plot_layout(guides = "collect") &
    theme(legend.position = 'right',legend.title = element_blank(),plot.margin = margin(1,2,0,0),legend.margin = margin(0,0,0,0))
}
save_as('dense-space', 25)

# space short
grouped|>
  filter(op=='ycsb_c',config_name!='dense1')|>
  mutate(config_name=fct_relevel(config_name,'dense2','dense3','hints'))|>
  ggplot() +
  theme_bw() +
  scale_color_brewer(palette = 'Dark2', name = "Configuration", labels = CONFIG_LABELS) +
  scale_x_continuous(
    name ='Density (%)',
    limits = c(0, 1),
    breaks = (0:4)*0.25,
    labels = label_percent(suffix=''),
    expand = expansion(add = 0)
  ) +
  guides(col = 'none')+
  expand_limits(y=0)+
  geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name)) +
  geom_text(
    aes(x = density, y = node_count * 4096 / 25e6 - 5, col = config_name,label=c('dense3'='fully d.','dense2'='semi d.','hints'='hints')[as.character(config_name)]),size=3,hjust='right',
  data=d_var_density|>
    filter(op=='ycsb_c',config_name!='dense1',density==c('hints'=0.96,'dense2'=0.52,'dense3'=0.98)[as.character(config_name)])
  )+
  scale_y_continuous(name = 'Space/Record', labels = label_bytes())+
  theme(
        axis.text.x = element_text(size = 8),
        axis.text.y = element_text(size = 8),
        axis.title.y = element_text(size = 8, hjust = 1),
        axis.title.x = element_text(size = 8),
        panel.spacing.x = unit(0.5, "mm"),
        plot.margin = margin(1, 8, 0, 0),
  )
save_as('dense-space', 20,w=34)



dense_joined|>filter(density==1)|>group_by(density,op,config_name.x)|>summarize(r=median(txs.x)/median(txs.y),n=n())
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=median(txs.x)/median(txs.y),n=n())|>group_by(op,config_name.x)|>slice_max(r)
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=median(txs.x)/median(txs.y),n=n())|>group_by(op,config_name.x)|>slice_min(r)
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=median(txs.x)/median(txs.y),n=n()) |>filter(r>1.1)|>group_by(op,config_name.x)|>slice_min(density)
dense_joined|>filter(density==1)|>mutate(r=1-node_count.x/node_count.y,op,config_name.x,.keep='none')|>arrange(r)
d_config_pivot|>
  filter(txs_dense3>txs_dense2 & density>0.25)|>
  group_by(op)|>
  slice_min(density)|>select(op,density)
d_var_density|>filter(density==0.3)|>group_by(density,op,config_name)|>summarize(br_miss=median(br_miss))
d_var_density|>group_by(op,config_name,density)|>summarize(br_miss=median(br_miss),.groups = 'drop_last')|>slice_max(br_miss)