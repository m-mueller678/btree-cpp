source("../common.R")

r <- bind_rows(
  # python3 R/eval-dense/dense-tasks.py |parallel -j1 --joblog joblog -- {1}| tee R/eval-dense/s1.csv
  #read_broken_csv('s1.csv'),
  # python3 R/eval-dense/task-sorted-insert.py |parallel -j1 --joblog joblog -- {1} > R/eval-dense/sorted.csv
  #read_broken_csv('sorted.csv'),
  # python3 R/eval-dense/partition-id-hint.py |parallel -j1 --joblog joblog -- {1} > R/eval-dense/partition-id-hint.csv
  #read_broken_csv('partition-id-hint.csv'),
  #christmas run
  read_broken_csv('dense-tasks-op2.csv.gz')
)

r|>group_by(config_name,data_name,op)|>count()|>arrange(n)|>filter(n!=ifelse(data_name=='int',50,10))

d <- r |>
  filter(op != "ycsb_e_init")|>
  augment()|>
  filter(scale > 0)

config_pivot <- d|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = mean)

dense_joined <- d|>
  filter(data_name!='partitioned_id')|>
  filter(config_name %in% c('dense1', 'dense2', 'dense3'))|>
  full_join(d|>filter(config_name == 'hints'), by = c('op', 'run_id', 'data_name'), relationship = 'many-to-one')

config_pivot|>
  filter(data_name == 'ints')|>
  select(data_name, op, br_miss_hints, br_miss_dense1, br_miss_dense2)

config_pivot|>
  filter(data_name == 'ints')|>
  mutate(data_name, op, r2=txs_dense2/txs_hints,r3=txs_dense3/txs_hints,.keep='none')

d|>
  filter(op %in% c("ycsb_c", "insert90", "scan", "sorted_insert"))|>
  filter(data_name == 'ints')|>
  group_by(op, config_name)|>
  summarise(txs = mean(txs))

config_pivot|>
  filter(data_name!='partitioned_id')|>
  mutate(c3 = txs_dense3/txs_hints-1,c2 = txs_dense2/txs_hints-1,c1 = txs_dense1/txs_hints-1)|>
  select(data_name,op,c3,c2,c1)|>
  arrange(data_name)

(
  dense_joined|>
    filter(op == 'ycsb_c', data_name == 'ints')|>
    ggplot() +
    geom_bar(aes(x = config_name.x, y = 1 - node_count.x / node_count.y), stat = 'summary', fun = mean) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
    labs(x = 'key set', y = "Relative") +
    coord_flip()
) | (
  dense_joined|>
    filter(op == 'ycsb_c', data_name == 'ints')|>
    ggplot() +
    geom_bar(aes(x = config_name.x, y = node_count.y * 4096 / 25000000 - node_count.x * 4096 / 25000000), stat = 'summary', fun = mean) +
    scale_y_continuous(
      labels = function(y) paste0(y, " B"),
      expand = expansion(mult = c(0, .1))) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank()) +
    coord_flip()
)

dense_joined|>
  filter(op == 'ycsb_c', data_name == 'ints')|>
  group_by(config_name.x)|>
  summarize(
    use_per_record = node_count.x * 4096 / 25000000,
    per_record = mean(node_count.y * 4096 / 25000000 - node_count.x * 4096 / 25000000),
    rel=1 - mean(node_count.x) / mean(node_count.y),
  )


d|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e", "sorted_insert"))|>
  filter(data_name == 'ints')|>
  ggplot() +
  geom_bar(aes(x = op, fill = config_name, y = txs), stat = 'summary', fun = mean, position = 'dodge')

dense_joined|>
  filter(config_name.x != "dense1")|>
  filter(data_name == 'ints')|>
  filter(op %in% c("ycsb_c", "insert90", "scan"))|>
  ggplot() +
  theme_bw() +
  facet_nested(. ~ config_name.x, labeller = labeller('config_name.x' = CONFIG_LABELS)) +
  geom_bar(aes(x = op, fill = op, y = txs.x / txs.y - 1), position = 'dodge', stat = 'summary', fun = mean) +
  geom_hline(yintercept = 0) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = 0.1), breaks = (0:20) * 0.3) +
  scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
  coord_cartesian(xlim = c(0.4, 3.6)) +
  guides(fill = guide_legend(ncol=1,title = NULL))+
  theme(
    axis.text.x = element_blank(), axis.ticks.x = element_blank(),
    legend.position = 'right',
    legend.margin = margin(-20,0,0,0),
  ) +
  scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
  labs(x = NULL, y = NULL, fill = 'Workload')
save_as('dense-speedup', 20)

d|>
  ggplot() +
  facet_nested(data_name ~ op) +
  geom_bar(aes(x = config_name, y = txs), stat = 'summary', fun = mean)

DENSE_RATE_SCALE = 1.5e7 * 1.25
SPACE_SCALE = 0.05
d|>
  filter(data_name == 'partitioned_id')|>
  mutate(dense_rate = nodeCount_Dense / leaf_count * DENSE_RATE_SCALE, space = leaf_count * 4096 * SPACE_SCALE)|>
  pivot_longer(c('dense_rate', 'txs', 'space'), values_to = 'v')|>
  ggplot() +
  geom_point(aes(x = data_size / ycsb_range_len, y = v, col = name)) +
  scale_x_log10(name = 'records per range', limits = c(10, 1e5)) +
  scale_y_continuous(name = "Throughput", labels = label_number(scale_cut = cut_si('op/s')), sec.axis = sec_axis(~. / DENSE_RATE_SCALE, name = "Share of Leaves using Dense Layouts", labels = scales::percent_format())) +
  scale_color_brewer(palette = 'Dark2') +
  expand_limits(y = 0) +
  theme(
    axis.title.y = element_text(color = "#1b9e77"),
    axis.title.y.right = element_text(color = "#d95f02"),
    #legend.position = "none",
  )

{
  label_data<-function(x_hints,x_dense)
    d|>filter(data_name=='partitioned_id', config_name=='dense3'& ycsb_range_len<x_dense |  config_name=='hints' & ycsb_range_len<x_hints )|>
      group_by(config_name,data_size,ycsb_range_len)|>
      summarize(txs=mean(txs),space=mean(node_count * 4096 /1e7),.groups = 'drop_last')|>
      slice_max(ycsb_range_len)
  common <- function(dense_only, has_x, name) d|>
    filter(!dense_only | config_name == 'dense3')|>
    filter(data_name == 'partitioned_id')|>
    ggplot() +
    theme_bw() +
    scale_color_brewer(palette = 'Dark2', name = "Configuration", labels = CONFIG_LABELS) +
    scale_x_log10(
      name = if (has_x) { 'records per range' }else { NULL },
      limits = c(10, 1e5),
      breaks = c(10, 1e2,1e3, 1e4),
      labels = label_number(scale_cut = cut_si('')),
      expand = expansion(add=0)
    ) +
    expand_limits(y = 0)+
    guides(col = 'none')+
    theme(
      plot.margin = margin(0,3,0,3),
      strip.text = element_text(size=8,margin = margin(2,2,2,2)),
    )

  tx <- common(FALSE, FALSE) +
    geom_line(aes(x = data_size / ycsb_range_len, y = txs, col = config_name),stat='summary',fun=mean) +
    geom_text(
      data = label_data(300,1000),
      aes(x = data_size / ycsb_range_len, y = txs,  label = CONFIG_LABELS[config_name],col=config_name),
      size = 3, hjust = "right", vjust = "bottom",
    )+
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s'))) +
    facet_nested(. ~ 'Throughput')
  space <- common(FALSE, FALSE) +
    geom_line(aes(x = data_size / ycsb_range_len, y = node_count * 4096 /1e7, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_bytes(),breaks = 20*(0:5),position = 'right') +
    geom_text(
      data = label_data(300,1000),
      aes(x = data_size / ycsb_range_len, y = space - ifelse(config_name=='hints',5,0), label = CONFIG_LABELS[config_name],col=config_name),
      size = 3, hjust = "right", vjust = "top"
    )+
    facet_nested(. ~ 'Space Per Record')
  (tx|space)
}
save_as('dense-partition', 25)

# tx ratio
config_pivot|>
  filter(data_name == 'partitioned_id')|>
  ggplot()+
  geom_point(aes(x=data_size/ycsb_range_len,y=txs_dense3/txs_hints))+
  scale_x_log10(limits = c(100,1e5))

# tx ratio
d|>
  filter(data_name == 'partitioned_id')|>
  ggplot()+
  geom_point(aes(x=data_size/ycsb_range_len,y=LLC_miss,col=config_name))+
  scale_x_log10(limits = c(100,1e5))


d|>
  ggplot() +
  geom_line(aes(x = data_size / ycsb_range_len, y = txs, col = config_name), stat = 'summary', fun = mean) +
  scale_x_log10() +
  expand_limits(y = 0)

d|>
  ggplot() +
  geom_point(aes(x = data_size / ycsb_range_len, y = 1 - nodeCount_Leaf / leaf_count, col = config_name)) +
  expand_limits(y = 0)

