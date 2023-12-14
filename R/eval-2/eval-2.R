source('../common.R')

r <- bind_rows(
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-2.csv
  # read_broken_csv('seq-zipf-2.csv')|>
  #   filter(!(config_name == 'hot' & (data_name %in% c('int', 'rng4'))))|>
  #   filter(config_name != 'adapt'),
  # hot build with integer specialization
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hot-n3-ycsb | tee R/eval-2/seq-zipf-hot.csv
  read_broken_csv('seq-zipf-hot.csv'),
  # sorted scans for hash vs hint
  # init is wrongly labeled as ycsb_c_init
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 501 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hints-n3-ycsb named-build/hash-n3-ycsb | tee R/eval-2/sorted-scan-seq.csv
  read_broken_csv('sorted-scan-seq.csv')|>filter(op == 'sorted_scan'),
  read_broken_csv('seq-zipf-dense3.csv'),
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 20) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/adapt-n3-ycsb | tee R/eval-2/seq-adapt-dense3.csv
  # read_broken_csv('seq-adapt-dense3.csv'),

  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 30) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-3.csv
  read_broken_csv('seq-zipf-3.csv')|>
    filter(!(config_name == 'hot' & (data_name %in% c('int', 'rng4'))))
)

COMMON_OPS <- c("ycsb_c", "ycsb_c_init", "ycsb_e")

d <- r |>
  filter(op != "ycsb_e_init")|>
  augment()|>
  filter(scale > 0)

d|>
  filter(data_name == 'urls')|>
  mutate(r = leaf_count / inner_count)|>
  select(r)

config_pivot <- d|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = mean)|>
  rowwise()|>
  mutate(
    txs_best = max(c_across(starts_with("txs_"))),
    node_count_best = min(c_across(starts_with("node_count_")))
  )|>
  ungroup()|>
  mutate(
    txs_best_speedup = txs_best / txs_baseline,
    best_space_saving = 1 - node_count_best / node_count_baseline,
  )

perf_common <- function(x = config_pivot, geom)
  x|>
    ggplot() +
    theme_bw() +
    facet_nested(. ~ data_name, independent = 'y', scales = 'free', labeller = labeller(
      op = OP_LABELS,
      data_name = DATA_LABELS,
    )) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = 0.1)) +
    scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
    coord_cartesian(xlim = c(0.4, 3.6)) +
    theme(
      axis.text.x = element_blank(), axis.ticks.x = element_blank(),
      legend.position = 'bottom',
      legend.text = element_text(margin = margin(t = 0)),
      legend.title = element_blank(),
      legend.margin = margin(0),
      legend.box.margin = margin(0),
      legend.spacing.x = unit(0, "mm"),
      legend.spacing.y = unit(-5, "mm"),
    ) +
    scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
    scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
    geom_point(aes(fill = op, col = op), x = 0, y = -1, size = 0) +
    labs(x = NULL, y = NULL, fill = 'Worload', col = 'Workload') +
    guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
    geom +
    geom_hline(yintercept = 0)

d|>
  filter(!(config_name %in% c('adapt', 'hot', 'art', 'tlx')))|>
  filter(op %in% c('ycsb_c', 'ycsb_c_init'))|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
  )) +
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('op/s'))
  ) +
  scale_fill_brewer(labels = CONFIG_LABELS, type = 'qual', palette = 'Dark2') +
  theme_bw() +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank()) +
  labs(x = NULL, y = "Throughput", fill = 'Configuration') +
  theme(
    text = element_text(size = 24),
    legend.position = 'bottom',
    panel.spacing.y = unit(1.5, "lines"),
  ) +
  guides(fill = guide_legend(ncol = 2))

d|>
  filter(!(config_name %in% c('adapt', 'hot', 'art', 'tlx')))|>
  filter(op %in% c('ycsb_c', 'ycsb_c_init'))|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free') +
  geom_bar(aes(x = config_name, y = node_count, fill = config_name), stat = 'summary', fun = mean) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_hue() +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank()) +
  labs(x = NULL, y = "Node Count", fill = 'Configuration')

config_pivot|>
  mutate(r = txs_hints / txs_baseline)|>
  select(data_name, op, txs_baseline, txs_hints, r)|>
  arrange(r)

config_pivot|>
  select(data_name, op, txs_baseline, txs_best, txs_best_speedup)|>
  arrange(op, txs_best_speedup)

config_pivot|>
  select(data_name, op, node_count_baseline, node_count_best, best_space_saving)|>
  arrange(best_space_saving)


# prefix

config_pivot|>
  mutate(r = keys_per_leaf_prefix / keys_per_leaf_baseline)|>
  select(data_name, op, keys_per_leaf_prefix, keys_per_leaf_baseline, r)|>
  arrange(r)

config_pivot|>
  mutate(r = 1 - node_count_prefix / node_count_baseline)|>
  select(data_name, op, node_count_prefix, node_count_baseline, r)|>
  arrange(r)

# absolute
config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  theme_bw() +
  geom_col(aes(x = data_name, y = node_count_baseline * 4096 / final_key_count_baseline - node_count_prefix * 4096 / final_key_count_prefix)) +
  scale_y_continuous(
    labels = label_bytes(),
    expand = expansion(mult = c(0, .1)),
  ) +
  labs(y = "Per Record") +
  theme(axis.title.y = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks.y = element_blank(),
        panel.spacing.x = unit(1.5, "lines"),
        text = element_text(size = 24)) +
  coord_flip()


config_pivot|>
  mutate(r = instr_prefix / instr_baseline - 1)|>
  select(data_name, op, instr_baseline, instr_prefix, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_prefix / L1_miss_baseline - 1)|>
  select(data_name, op, L1_miss_baseline, L1_miss_prefix, r)|>
  arrange(r)

config_pivot|>
  mutate(r = LLC_miss_prefix / LLC_miss_baseline - 1)|>
  select(data_name, op, LLC_miss_baseline, LLC_miss_prefix, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_prefix / br_miss_baseline - 1)|>
  select(data_name, op, br_miss_baseline, br_miss_prefix, r)|>
  arrange(r)

perf_common(geom = geom_col(aes(x = op, fill = op, y = txs_prefix / txs_baseline - 1)))
save_as('prefix-speedup', h = 40)

config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  theme_bw() +
  geom_col(aes(x = data_name, y = 1 - node_count_prefix / node_count_baseline)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)), breaks = (0:7) * 0.1) +
  scale_x_discrete(labels = DATA_LABELS) +
  labs(x = 'key set', y = NULL) +
  theme(text = element_text(size = 24)) +
  coord_flip()

config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = 1 - node_count_prefix / node_count_baseline)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  labs(x = 'key set', y = "Space Savings") +
  coord_flip()

# heads
perf_common() +
  geom_col(aes(x = op, y = txs_heads / txs_prefix - 1, fill = op)) +
  geom_hline(yintercept = 0)

config_pivot|>
  filter(op == 'ycsb_c')|>
  mutate(d = node_count_heads * 4096 / final_key_count_heads - node_count_prefix * 4096 / final_key_count_prefix)|>
  select(d, data_name)

((
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    theme_bw() +
    geom_col(aes(x = data_name, y = node_count_heads / node_count_prefix - 1)) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)), breaks = (0:2) * 0.1) +
    scale_x_discrete(labels = DATA_LABELS) +
    labs(x = 'key set', y = "Relative") +
    theme(text = element_text(size = 24)) +
    coord_flip()
) | (
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    theme_bw() +
    geom_col(aes(x = data_name, y = node_count_heads * 4096 / final_key_count_heads - node_count_prefix * 4096 / final_key_count_prefix)) +
    scale_y_continuous(
      labels = label_bytes(),
      expand = expansion(mult = c(0, .1)),
      breaks = c(0, 2, 4, 6)) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          panel.spacing.x = unit(1.5, "lines"),
          text = element_text(size = 24)) +
    coord_flip()
))

config_pivot|>
  mutate(r = 1 - keys_per_leaf_heads / keys_per_leaf_prefix)|>
  select(data_name, op, keys_per_leaf_heads, keys_per_leaf_prefix, r)|>
  arrange(r)

config_pivot|>
  mutate(r = txs_heads / txs_prefix - 1)|>
  select(data_name, op, txs_prefix, txs_heads, r)|>
  arrange(r)

config_pivot|>
  mutate(r = instr_heads / instr_prefix - 1)|>
  select(data_name, op, instr_prefix, instr_heads, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_heads / L1_miss_prefix - 1)|>
  select(data_name, op, L1_miss_prefix, L1_miss_heads, r)|>
  arrange(r)

config_pivot|>
  mutate(r = LLC_miss_heads / LLC_miss_prefix - 1)|>
  select(data_name, op, LLC_miss_prefix, LLC_miss_heads, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_heads / br_miss_prefix - 1)|>
  select(data_name, op, br_miss_prefix, br_miss_heads, r)|>
  arrange(r)

d|>count(op, data_name, config_name)|>glimpse()

d|>
  filter(config_name %in% c('heads', 'prefix'))|>
  filter(data_name %in% c('ints', 'sparse'))|>
  ggplot() +
  facet_nested(config_name ~ .) +
  geom_freqpoly(aes(x = leaf_count * 4096 / final_key_count, col = data_name))

d|>
  filter(config_name == 'heads')|>
  filter(data_name %in% c('ints', 'sparse'))|>
  select(data_name, leaf_count)|>
  arrange(data_name, leaf_count)|>
  View()


config_pivot|>
  filter(op == 'ycsb_c')|>
  mutate(
    space_per_key_heads = leaf_count_heads * (4096 - 32) / final_key_count_heads,
    space_per_key_prefix = leaf_count_prefix * (4096 - 32) / final_key_count_prefix,
  )|>
  select(avg_key_size_baseline, data_name, space_per_key_prefix, space_per_key_heads)

# hints

perf_common() +
  geom_col(aes(x = op, fill = op, y = txs_hints / txs_heads - 1)) +
  geom_hline(yintercept = 0)

config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = node_count_hints / node_count_heads - 1)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  labs(x = 'key set', y = "Space Overhead") +
  coord_flip()

config_pivot|>
  mutate(r = txs_hints / txs_heads - 1)|>
  select(data_name, op, r)|>
  arrange(op, r)

config_pivot|>
  mutate(r = instr_hints / instr_heads - 1)|>
  select(data_name, op, instr_heads, instr_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = instr_hints - instr_heads)|>
  select(data_name, op, instr_heads, instr_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_hints / L1_miss_heads - 1)|>
  select(data_name, op, L1_miss_heads, L1_miss_hints, r)|>
  arrange(op, r)

config_pivot|>
  mutate(r = LLC_miss_hints / LLC_miss_heads - 1)|>
  select(data_name, op, LLC_miss_heads, LLC_miss_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_hints / br_miss_heads - 1)|>
  select(data_name, op, br_miss_heads, br_miss_hints, r)|>
  arrange(r)

# hash
perf_common(geom = geom_col(aes(x = op, y = txs_hash / txs_hints - 1, fill = op))) +
  coord_cartesian(xlim = c(0.4, 4.6))
save_as('hash-speedup', 40)

{ (
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    theme_bw() +
    geom_col(aes(x = data_name, y = node_count_hash / node_count_prefix - 1)) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)), breaks = (0:4) * 0.025) +
    scale_x_discrete(labels = DATA_LABELS) +
    labs(x = 'key set', y = "Relative") +
    theme(text = element_text(size = 24)) +
    coord_flip()
) | (
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    theme_bw() +
    geom_col(aes(x = data_name, y = node_count_hash * 4096 / final_key_count_hash - node_count_prefix * 4096 / final_key_count_prefix)) +
    scale_y_continuous(
      labels = label_bytes(),
      expand = expansion(mult = c(0, .1)),
      breaks = (0:2)) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          panel.spacing.x = unit(1.5, "lines"),
          text = element_text(size = 24)) +
    coord_flip()
) }


config_pivot|>
  mutate(r = txs_hash / txs_hints - 1)|>
  select(data_name, op, txs_hints, txs_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = txs_hash / txs_prefix - 1)|>
  select(data_name, op, r)|>
  arrange(data_name, r)

config_pivot|>
  mutate(r = node_count_hash / node_count_prefix - 1)|>
  select(data_name, op, r)|>
  filter(op == 'ycsb_c')|>
  arrange(r)

config_pivot|>
  mutate(r = node_count_hash / node_count_hints - 1)|>
  select(data_name, op, r)|>
  filter(op == 'ycsb_c')|>
  arrange(r)

config_pivot|>
  mutate(r = instr_hash / instr_hints - 1)|>
  select(data_name, op, instr_hints, instr_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = instr_hash - instr_hints)|>
  select(data_name, op, instr_hints, instr_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_hash / L1_miss_hints - 1)|>
  select(data_name, op, L1_miss_hints, L1_miss_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = LLC_miss_hash / LLC_miss_hints - 1)|>
  select(data_name, op, LLC_miss_hints, LLC_miss_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_hash / br_miss_hints - 1)|>
  select(data_name, op, br_miss_hints, br_miss_hash, r)|>
  arrange(r)

# hash head space
d|>filter(op=='ycsb_c',config_name %in% c('heads','prefix'),data_name=='ints')|>group_by(config_name)|>summarize(c=mean(nodeCount_Leaf))

{
  space_data <- config_pivot|>
    filter(op == 'ycsb_c')|>
    transmute(
      data_name,
      abs_heads = (node_count_heads - node_count_prefix) * 4096 / data_size,
      abs_hash = (node_count_hash - node_count_prefix) * 4096 / data_size,
      rel_heads = node_count_heads / node_count_prefix - 1,
      rel_hash = node_count_hash / node_count_prefix - 1,
    )|>
    pivot_longer(!any_of('data_name'), names_sep = '_', names_to = c('ref', 'config'))|>
    mutate(config = factor(config,levels = CONFIG_NAMES))

  plot <- function(ref_filter) {
    space_data|>
      filter(ref == ref_filter)|>
      ggplot() +
      theme_bw() +
      geom_col(aes(x = data_name, y = value, fill = config), position = position_dodge2(reverse = TRUE)) +
      scale_fill_brewer(palette = 'Dark2',labels = CONFIG_LABELS,name=NULL)+
      scale_x_discrete(labels = DATA_LABELS, name = 'key set') +
      theme(legend.position = 'bottom')+
      coord_flip()
  }
  hide_y<-theme(
    axis.title.y=element_blank(),
    axis.ticks.y=element_blank(),
    axis.text.y=element_blank(),
  )
  pr <- plot('rel') +
    scale_y_continuous(
      labels = label_percent(),
      name = 'Relative',
      expand = expansion(mult = c(0, .1)),
      breaks = (0:2) * 0.1
    )
  pa <- plot('abs') +
    scale_y_continuous(
      labels = label_bytes(),
      name = 'Per Record',
      expand = expansion(mult = c(0, .1)),
      breaks = c(0, 2, 4, 6)
    )

  (pr + (pa+hide_y)) + plot_layout(guides = 'collect')&theme(legend.position = 'bottom',legend.margin = margin(0,0,0,0),plot.margin = margin(0,0,0,0),legend.key.size = unit(4,'mm'))
  save_as('hash-head-space-overhead',30)
}


#integer separators
perf_common(config_pivot|>filter(op != 'sorted_scan', data_name %in% c('ints', 'sparse')), geom_col(aes(x = op, y = txs_inner / txs_hints - 1, fill = op))) +
  facet_nested(. ~ data_name, labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
  )) +
  theme(legend.position = 'right')
save_as('inner-speedup', 20)

d|>
  filter(config_name == 'inner', op == 'ycsb_c', data_name %in% c('ints', 'sparse'), nodeCount_Inner > 0)|>
  View()

d|>
  filter(config_name == 'inner', op == 'ycsb_c')|>
  mutate(
    head_count = nodeCount_Head4 + nodeCount_Head8,
    head_share = head_count / inner_count,
    head4_share = nodeCount_Head4 / inner_count,
  )|>
  select(op, data_name, head_count, head_share, head4_share)|>
  ggplot() +
  geom_violin(aes(x = data_name, y = head4_share)) +
  coord_cartesian(ylim = c(0.9, 1))

config_pivot|>
  mutate(r = instr_inner / instr_hints - 1)|>
  select(data_name, op, instr_hints, instr_inner, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_inner / L1_miss_hints - 1)|>
  select(data_name, op, L1_miss_hints, L1_miss_inner, r)|>
  arrange(r)

config_pivot|>
  mutate(r = LLC_miss_inner / LLC_miss_hints - 1)|>
  select(data_name, op, LLC_miss_hints, LLC_miss_inner, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_inner / br_miss_hints - 1)|>
  select(data_name, op, br_miss_hints, br_miss_inner, r)|>
  arrange(r)

# in-memory

in_mem_plot <- function(show_op, configs) {

  make_plot <- function(data) {
    data|>
      group_by(config_name, op, data_name)|>
      summarise(txs = mean(txs) / 1e6)|>
      mutate(
        g = unname(configs[as.character(config_name)])
      )|>
      group_by(op, g, data_name)|>
      arrange(txs, .by_group = TRUE)|>
      mutate(
        hwidth = 0.55 - rank(txs) * 0.1,
        bar_bottom = lag(txs, default = 0),
        bar_top = txs,
      )|>
      ggplot() +
      theme_bw() +
      facet_nested(
        if (length(show_op) > 1) { op ~ data_name } else { . ~ data_name },
        labeller = labeller(
          data_name = DATA_LABELS,
          op = OP_LABELS,
        ),
        scales = 'free_y'
      ) +
      geom_rect(aes(ymin = bar_bottom, ymax = bar_top, xmin = g - hwidth, xmax = g + hwidth, , fill = config_name)) +
      scale_x_continuous(
        breaks = NULL
      ) +
      scale_y_continuous(
        expand = expansion(mult = c(0, .1)),
        name = NULL
      ) +
      guides(fill = guide_legend(override.aes = list(size = 0.1), title.position = "top", title.hjust = 0.5, nrow = 2)) +
      scale_fill_brewer(palette = "Dark2", labels = CONFIG_LABELS) +
      labs(x = NULL, y = "Throughput", fill = 'Configuration') +
      theme(legend.position = 'bottom',
            legend.title = element_blank(),
            axis.text.x = element_blank(),
            panel.spacing.x = unit(0.1, "lines"),
            panel.spacing.y = unit(1, "lines")
      )
  }

  op_data <- d|>
    filter(config_name %in% names(configs))|>
    filter(op %in% show_op)|>
    mutate(text = data_name %in% c('urls', 'wiki'))
  no_facet <- theme(
    strip.background.y = element_blank(),
    strip.text.y = element_blank()
  )
  ((make_plot(op_data|>filter(text)) + no_facet) | make_plot(op_data|>filter(!text))) + plot_layout(guides = "collect") & theme(legend.position = 'bottom')
}

HOT_ART_CONFIGS <- c('baseline' = 2, 'dense3' = 2, 'hash' = 2, 'art' = 3, 'hot' = 3)
TLX_CONFIGS <- c('baseline' = 2, 'dense3' = 2, 'hash' = 2, 'tlx' = 3)
ALL_CONFIGS <- c(HOT_ART_CONFIGS, 'tlx' = 4)

in_mem_plot(COMMON_OPS, ALL_CONFIGS)
save_as('mem-trie', 90)
in_mem_plot('ycsb_c_init')
in_mem_plot('ycsb_e')

config_pivot|>
  transmute(r = txs_art / txs_dense3, op, data_name)|>
  arrange(op, data_name)
config_pivot|>
  transmute(r = txs_hash / txs_art, op, data_name)|>
  arrange(op, data_name)
config_pivot|>
  transmute(r = txs_dense3 / pmax(txs_hot, txs_art) - 1, op, data_name)|>
  arrange(op, data_name)
config_pivot|>
  transmute(r1 = txs_baseline / txs_tlx, r2 = pmax(txs_baseline, txs_dense3, txs_hash) / txs_tlx, r3 = txs_baseline / txs_tlx - 1, op, data_name)|>
  arrange(data_name, op)

d|>
  filter(config_name %in% c('baseline', 'art', 'hot', 'dense1', 'hash'), op %in% COMMON_OPS)|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free', independent = 'y') +
  geom_bar(aes(x = config_name, y = txs, fill = config_name), stat = 'summary', fun = mean) +
  scale_x_discrete(labels = CONFIG_LABELS) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_brewer(palette = "Dark2") +
  labs(x = NULL, y = "Throughput", fill = 'Configuration') +
  theme_bw() +
  theme(axis.text.x = element_blank(), legend.position = "bottom", legend.justification = "center")

d|>
  filter(config_name %in% c('baseline', 'art', 'hot', 'dense1', 'hash'), op %in% COMMON_OPS)|>
  ggplot() +
  facet_wrap(~op + data_name, scales = 'free') +
  geom_bar(aes(x = config_name, y = LLC_miss, fill = config_name), stat = 'summary', fun = mean) +
  scale_x_discrete(labels = CONFIG_LABELS) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_brewer(palette = "Dark2") +
  labs(x = NULL, y = "Count", fill = 'Configuration') +
  theme(axis.text.x = element_blank())

# adapt
config_pivot|>
  filter(op %in% COMMON_OPS)|>
  mutate(
    ref_txs_best = pmax(txs_hash, txs_hints, txs_dense3),
    ref_txs_worst = pmin(txs_hash, txs_hints, txs_dense3),
    ref_txs_hash = txs_hash,
    ref_txs_dense3 = txs_dense3,
    # ref_txs_hints = txs_hints,
  )|>
  pivot_longer(contains('ref_txs_'), names_to = 'reference_name', values_to = 'reference_value', names_prefix = 'ref_txs_')|>
  transmute(op,data_name,r=txs_adapt/reference_value-1,reference_name)|>arrange(reference_name,op,r)|>View()
  ggplot() +
  theme_bw() +
  facet_nested(reference_name ~ data_name, scales = 'free', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    reference_name = CONFIG_LABELS,
  )) +
  scale_y_continuous(labels = label_percent(style_positive = "plus"), expand = expansion(mult = 0.1)) +
  scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
  coord_cartesian(xlim = c(0.4, 3.6)) +
  theme(
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank(),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(0),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    strip.text.y = element_text(size=6),
  ) +
  scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
  scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
  geom_point(aes(fill = op, col = op), x = 0, y = -1, size = 0) +
  labs(x = NULL, y = NULL, fill = 'Worload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
  geom_col(aes(x = op, fill = op, y = txs_adapt / reference_value - 1)) +
  geom_hline(yintercept = 0)
save_as('adapt-perf-a', 50)

in_mem_plot(COMMON_OPS, c('hints' = 1, 'hash' = 1, 'dense3' = 1, 'adapt' = 2))
save_as('adapt-perf-c', 90)


in_mem_plot(COMMON_OPS, c('hints' = 1, 'hash' = 1, 'dense3' = 1, 'adapt' = 2))
save_as('adapt-perf', 90)