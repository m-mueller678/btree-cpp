source("../common.R")

# python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee R/eval-dense/var-density.csv
r_var_density <- bind_rows(
  # python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee -a R/eval-dense/var-density.csv
  read_broken_csv('var-density.csv.gz'),
  read_broken_csv('var-density-2.csv'),
  # python3 R/eval-dense/var-density-e.py |parallel -j1 --joblog joblog -- {1}| tee -a R/eval-dense/var-density-e.csv
  read_broken_csv('var-density-e.csv.gz'),
)

d_var_density <- r_var_density|>augment()

d_config_pivot<-d_var_density|>pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = mean)

d_var_density|>group_by(op,density,config_name)|>count()|>filter(n!=10)

dense_joined <- d_var_density|>
  filter(config_name %in% c('dense1', 'dense2', 'dense3'))|>
  full_join(d_var_density|>filter(config_name == 'hints'), by = c('op', 'run_id', 'data_name','density'), relationship = 'many-to-one')

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
    geom_line(aes(x = density, y = txs, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))+
    facet_nested(. ~ 'ycsb-c')
  txi <- common('ycsb_c_init',FALSE) +
    facet_nested(. ~ op, labeller = labeller(
      op = OP_LABELS,
    ),scales='free_y',independent = 'y') +
    geom_line(aes(x = density, y = txs, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')),position = 'right')+
    facet_nested(. ~ 'insert')
  space <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_bytes()) +
    facet_nested(. ~ 'space per record')
  dense <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = (nodeCount_Dense+nodeCount_Dense2) / leaf_count, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_percent(),position = 'right') +
    facet_nested(. ~ 'dense leaf share')
  (txc+txi+space+dense) + plot_layout(guides = "collect") &
    theme(legend.position = 'bottom',legend.title = element_blank(),plot.margin = margin(1),legend.margin = margin(-15,0,0,0))
}
save_as('var-density', 70)

# speedup

dense_joined|>
  filter(config_name.x!='dense1')|>
  filter(op!='ycsb_e_init')|>
  ggplot() +
  theme_bw() +
  scale_color_brewer(palette = 'Dark2', name = NULL, labels = CONFIG_LABELS) +
  scale_x_continuous(
    name=NULL,
    limits = c(0, 1),
    breaks = (0:1)*0.5,
    labels = label_percent(),
    expand = expansion(add = 0)
  ) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),name=NULL) +
  guides(col = guide_legend(override.aes = list(size = 1)))+
  expand_limits(y=0)+
  facet_nested(~op, labeller = labeller(
    op = OP_LABELS,
  ))+
  geom_line(aes(density,txs.x/txs.y-1,col=config_name.x),stat='summary',fun=mean)+
  theme(legend.position = 'right',legend.margin = margin(-10,0,0,-10),plot.margin = margin(0))
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
    geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_bytes())
  dense <- common('ycsb_c', TRUE) +
    geom_line(aes(x = density, y = (nodeCount_Dense+nodeCount_Dense2) / leaf_count, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_percent(),breaks=(0:4)*0.25,position = 'right')
  (space|dense) + plot_layout(guides = "collect") &
    theme(legend.position = 'right',legend.title = element_blank(),plot.margin = margin(1,2,0,0),legend.margin = margin(0,0,0,0))
}
save_as('dense-space', 25)

dense_joined|>filter(density==0.99)|>group_by(density,op,config_name.x)|>summarize(r=mean(txs.x/txs.y),n=n())
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=mean(txs.x/txs.y),n=n())|>group_by(op,config_name.x)|>slice_max(r)
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=mean(txs.x/txs.y),n=n())|>group_by(op,config_name.x)|>slice_min(r)
dense_joined|>group_by(density,op,config_name.x)|>summarize(r=mean(txs.x/txs.y),n=n()) |>filter(r>1.1)|>group_by(op,config_name.x)|>slice_min(density)
dense_joined|>filter(density==0.99)|>group_by(density,op,config_name.x)|>summarize(r=mean(1-node_count.x/node_count.y),n=n())
d_config_pivot|>
  filter(txs_dense3>txs_dense2 & density>0.25)|>
  group_by(op)|>
  slice_min(density)
d_var_density|>filter(density==0.99)|>group_by(density,op,config_name)|>summarize(br_miss=mean(br_miss))
d_config_pivot|>group_by(op)|>slice_max(br_miss_hints)|>select(density,op,contains('br_miss'))