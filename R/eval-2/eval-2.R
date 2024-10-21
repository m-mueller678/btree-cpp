source('../common.R')

r <- bind_rows(
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-2.csv
  # read_broken_csv('seq-zipf-2.csv')|>
  #   filter(!(config_name == 'hot' & (data_name %in% c('int', 'rng4'))))|>
  #   filter(config_name != 'adapt'),
  # hot build with integer specialization
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hot-n3-ycsb | tee R/eval-2/seq-zipf-hot.csv
  #read_broken_csv('seq-zipf-hot.csv'),
  # sorted scans for hash vs hint
  # init is wrongly labeled as ycsb_c_init
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 501 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hints-n3-ycsb named-build/hash-n3-ycsb | tee R/eval-2/sorted-scan-seq.csv
  #read_broken_csv('sorted-scan-seq.csv')|>filter(op == 'sorted_scan'),

  # obsoleted by seq-zipf-3
  #read_broken_csv('seq-zipf-dense3.csv'),

  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 20) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/adapt-n3-ycsb | tee R/eval-2/seq-adapt-dense3.csv
  # read_broken_csv('seq-adapt-dense3.csv'),

  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 30) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-3.csv
  #read_broken_csv('seq-zipf-3.csv')|>
  #  filter(!(config_name == 'hot' & (data_name %in% c('int', 'rng4')))),
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 30) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/adapt2-n3-ycsb | tee R/eval-2/adapt2-fed65b81398dc6b.csv
  #read_broken_csv('adapt2-fed65b81398dc6b.csv.gz')
  #read_broken_csv('adapt2-fed65b81398dc6b.csv.gz')

  #christmas run
  read_broken_csv('re-eval.csv.gz'),

  #dense
  read_broken_csv('re-eval-dense.csv.gz')|>filter(config_name != 'dense1'),

  # wormhole
  read_broken_csv('eval-wh.csv.gz'),
  #lits
  read_broken_csv('lits-train5.csv.gz')|>mutate(config_name='lits2'),
  read_broken_csv('lits-inline.csv.gz'),
)

COMMON_OPS <- c("ycsb_c", "insert90", "scan")

d <- r |>
  filter(op != "ycsb_e_init")|>
  # lits crashes sometimes, we select the first run ids that exist
  group_by(op,config_name,data_name)|>arrange(run_id)|>slice_head(n=5)|>ungroup()|>
  augment()|>
  filter(scale > 0)

d|>filter(config_name=='lits',op=='ycsb_c')|>View()

d|>
  filter(data_name == 'urls')|>
  mutate(r = leaf_count / inner_count)|>
  select(r)

config_pivot <- d|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = median)|>
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
      legend.margin = margin(-10, 0, 0, 0),
      legend.box.margin = margin(0),
      legend.spacing.x = unit(0, "mm"),
      legend.spacing.y = unit(-5, "mm"),
      plot.margin = margin(0),
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
  arrange(op, r)

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
  arrange(r)|>print(n=50)

options(pillar.sigfig = 7)
config_pivot|>
  filter(op == 'ycsb_c')|>
  mutate(r = 1 - node_count_prefix / node_count_baseline)|>
  select(data_name, op, node_count_prefix, node_count_baseline, r)|>
  arrange(r)|>
  print()

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

perf_common(config_pivot|>filter(op %in% COMMON_OPS), geom = geom_col(aes(x = op, fill = op, y = txs_prefix / txs_baseline - 1))) + expand_limits(y = 0.06)
save_as('prefix-speedup', h = 30)


{

  space_plot <- function(left) {
    data_filter <- if (left) { c('urls', 'wiki') }else { c('ints', 'sparse') }
    config_pivot|>
      filter(op == 'ycsb_c', data_name %in% data_filter)|>
      ggplot() +
      theme_bw() +
      geom_col(aes(x = data_name, y = 1 - node_count_prefix / node_count_baseline, fill = data_name)) +
      scale_fill_manual(palette = function(x) brewer_pal(palette = 'Dark2')(4)[(3:4) - left * 2]) +
      guides(fill = 'none') +
      scale_y_continuous(labels = label_percent(), expand = expansion(mult = 0),limits = c(0,0.7)) +
      scale_x_discrete(limits = {
        l <- rev(levels(config_pivot$data_name))
        l[l %in% unique((config_pivot|>filter(!is.na(txs_prefix)))$data_name) & l %in% data_filter]
      }, labels = DATA_LABELS) +
      labs(x = NULL, y = NULL) +
      coord_flip()
  }

  space_plot(TRUE) +
    space_plot(FALSE) +
    plot_annotation(caption = 'Space Savings') &
    theme(plot.caption = element_text(size = 8, hjust = 0.5, margin = margin(0, 0, 1, 0)),
          plot.margin = margin(1, 5, 1, 5),
          axis.title.x = element_text(size = 8),
          strip.text = element_blank(),
          strip.background = element_blank())
}
save_as('prefix-space', 14)

# heads

{

  space_plot <- function(left) {
    data_filter <- if (left) { c('urls', 'wiki') }else { c('ints', 'sparse') }
    config_pivot|>
      filter(op == 'ycsb_c', data_name %in% data_filter)|>
      ggplot() +
      theme_bw() +
      geom_col(aes(x = data_name, y = node_count_heads / node_count_prefix - 1, fill = data_name)) +
      scale_fill_manual(palette = function(x) brewer_pal(palette = 'Dark2')(4)[(3:4) - left * 2]) +
      guides(fill = 'none') +
      scale_y_continuous(labels = label_percent(), expand = expansion(mult = 0),limits = c(0,0.25), breaks = (0:10) * 0.1) +
      scale_x_discrete(limits = {
        l <- rev(levels(config_pivot$data_name))
        l[l %in% unique((config_pivot|>filter(!is.na(txs_prefix)))$data_name) & l %in% data_filter]
      }, labels = DATA_LABELS) +
      labs(x = NULL, y = NULL) +
      coord_flip()
  }

  space_plot(TRUE) +
    space_plot(FALSE) +
    plot_annotation(caption = 'Space Overhead') &
    theme(plot.caption = element_text(size = 8, hjust = 0.5, margin = margin(0, 0, 1, 0)),
          plot.margin = margin(1, 5, 1, 5),
          axis.title.x = element_text(size = 8),
          strip.text = element_blank(),
          strip.background = element_blank())
}
save_as('heads-space', 14)


{
  # generate plots, then generate heads-int-size-plot (insert-log), then create patchwork

  space_base <- config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    theme_bw() +
    scale_fill_brewer(palette = 'Dark2') +
    guides(fill = 'none') +
    theme(
      axis.text.x = element_text(angle = 25, hjust = 1),
      axis.title.y = element_text(size = 8, hjust = 1),
      axis.title.x = element_text(size = 8),
    ) +
    scale_x_discrete(labels = DATA_LABELS)
  relative <- space_base +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)), breaks = (0:10) * 0.1) +
    labs(x = 'Key Set', y = 'Space Overhead') +
    geom_col(aes(x = data_name, y = node_count_heads / node_count_prefix - 1, fill = data_name))

  # absolute<-space_base+
  #   scale_y_continuous(labels = label_bytes(), expand = expansion(mult = c(0, .1)),position = 'right') +
  #   labs(x = NULL, y = NULL)+
  #   geom_col(aes(x = data_name, y = (node_count_heads-node_count_prefix)*4096/data_size, fill = data_name))

  (separator_misery + relative) & theme(plot.margin = margin(3, 5, 0, 5))

  save_as('heads-space', 25)
}


config_pivot|>

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
  arrange(r)|>print(n=12)

config_pivot|>
  mutate(r = txs_heads / txs_prefix - 1)|>
  select(data_name, op, txs_prefix, txs_heads, r)|>
  arrange(r)|>
  print(n = 50)

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
    d = space_per_key_heads - space_per_key_prefix,
    data_name,
    .keep = 'none',
  )

# hints

perf_common(config_pivot|>filter(op %in% COMMON_OPS), geom_col(aes(x = op, fill = op, y = txs_hints / txs_heads - 1))) + expand_limits(y = 0.06)
save_as('hints-speedup', 30)

config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = node_count_hints / node_count_heads - 1)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  labs(x = 'key set', y = "Space Overhead") +
  coord_flip()

config_pivot|>
  mutate(r = node_count_hints / node_count_heads - 1)|>
  select(data_name, op, r)|>
  arrange(op, r)


config_pivot|>
  mutate(r = txs_hints / txs_heads - 1)|>
  select(data_name, op, r)|>
  arrange(op, r)

config_pivot|>
  mutate(r = instr_hints / instr_heads - 1)|>
  select(data_name, op, instr_heads, instr_hints, r)|>
  arrange(r)|>
  print(n = 13)

config_pivot|>
  mutate(r = instr_hints - instr_heads)|>
  select(data_name, op, instr_heads, instr_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_hints / L1_miss_heads - 1)|>
  select(data_name, op, L1_miss_heads, L1_miss_hints, r)|>
  arrange(op, r)|>
  filter(!is.na(r))

config_pivot|>
  mutate(r = LLC_miss_hints / LLC_miss_heads - 1)|>
  select(data_name, op, LLC_miss_heads, LLC_miss_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_hints / br_miss_heads - 1)|>
  select(data_name, op, br_miss_heads, br_miss_hints, r)|>
  arrange(r)

# prefix heads hints combined

config_pivot|>
  filter(op %in% COMMON_OPS)|>
  mutate(speedup_prefix = txs_prefix / txs_baseline,
         speedup_heads = txs_heads / txs_prefix,
         speedup_hints = txs_hints / txs_heads,
         data_name, op, .keep = 'none')|>
  pivot_longer(contains('speedup_'), names_prefix = 'speedup_')|>
  mutate(config = factor(name, levels = names(CONFIG_LABELS)))|>
  ggplot() +
  theme_bw() +
  facet_nested(~config + data_name, labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    config = CONFIG_LABELS,
  )) +
  scale_y_continuous(expand = expansion(mult = 0),limits = c(-10,60), breaks = c(0,20,40,60)) +
  scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
  coord_cartesian(xlim = c(0.4, 3.6)) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_blank(),
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8),
    panel.spacing.x = unit(1, "mm"),
    axis.ticks.x = element_blank(),
    legend.position = 'right',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(-10, 0, 0, 0),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 0, 0, 1),
  ) +
  scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
  scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
  geom_point(aes(fill = op, col = op), x = 0, y = -1, size = 0) +
  labs(x = NULL, y = 'op/s Increase (%)', fill = 'Workload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)),
         fill = 'none') +
  geom_col(aes(x = op, fill = op, y = (value - 1) * 100)) +
  expand_limits(y = 6) +
  geom_hline(yintercept = 0)
save_as('phh-speedup', 30, w = 180)

# hash

config_pivot|>
  filter(op %in% c('ycsb_c', 'insert90', 'ycsb_e', 'sorted_scan'))|>
  ggplot() +
  theme_bw() +
  facet_nested(. ~ data_name, labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
  )) +
  scale_y_continuous(expand = expansion(mult = 0),limits=c(-30,45)) +
  scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
  coord_cartesian(xlim = c(0.4, 3.6)) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_blank(),
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8),
    panel.spacing.x = unit(1, "mm"),
    axis.ticks.x = element_blank(),
    legend.position = 'right',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(0, 0, 0, 0),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 0, 0, 1),
  ) +
  scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
  scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
  geom_point(aes(fill = op, col = op), x = 0, y = -1, size = 0) +
  labs(x = NULL, y = 'op/s Increase (%)', fill = 'Worload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
  geom_col(aes(x = op, y = (txs_hash / txs_hints - 1) * 100, fill = op)) +
  geom_hline(yintercept = 0) +
  coord_cartesian(xlim = c(0.4, 4.6))
save_as('hash-speedup', 25)

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
  arrange(op, r)

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
d|>
  filter(op == 'ycsb_c', config_name %in% c('heads', 'prefix'), data_name == 'ints')|>
  group_by(config_name)|>
  summarize(c = mean(nodeCount_Leaf))

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
    mutate(config = factor(config, levels = CONFIG_NAMES))

  plot <- function(ref_filter) {
    space_data|>
      filter(ref == ref_filter)|>
      ggplot() +
      theme_bw() +
      geom_col(aes(x = data_name, y = value, fill = config), position = position_dodge2(reverse = TRUE)) +
      scale_fill_brewer(palette = 'Dark2', labels = CONFIG_LABELS, name = NULL) +
      scale_x_discrete(labels = DATA_LABELS, name = NULL) +
      theme(legend.position = 'bottom') +
      coord_flip() +
      theme(strip.text = element_text(size = 8, margin = margin(2, 2, 2, 2)))
  }

  hide_y <- theme(
    axis.title.y = element_blank(),
    axis.ticks.y = element_blank(),
    axis.text.y = element_blank(),
  )
  pr <- plot('rel') +
    facet_wrap(~'Relative') +
    scale_y_continuous(
      labels = label_percent(),
      name = NULL,
      expand = expansion(mult = c(0, .1)),
      breaks = (0:2) * 0.1
    )
  pa <- plot('abs') +
    facet_wrap(~'Per Record') +
    scale_y_continuous(
      labels = label_bytes(),
      name = NULL,
      expand = expansion(mult = c(0, .1)),
      breaks = c(0, 2, 4, 6)
    )

  (pr + (pa + hide_y)) + plot_layout(guides = 'collect') & theme(legend.position = 'right', legend.margin = margin(0, 0, 0, 0), plot.margin = margin(0, 0, 0, 0), legend.key.size = unit(4, 'mm'))
  save_as('hash-head-space-overhead', 20)
}


#integer separators
perf_common(config_pivot|>filter(op %in% COMMON_OPS, data_name %in% c('ints', 'sparse')), geom_col(aes(x = op, y = txs_inner / txs_hints - 1, fill = op))) +
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
  group_by(data_name)|>
  summarize(
    share = mean((nodeCount_Head8 + nodeCount_Head4) / inner_count),
    share4 = mean((nodeCount_Head4) / inner_count),
    n = n()
  )

config_pivot|>
  mutate(r = txs_inner / txs_hints - 1)|>
  select(data_name, op, r)|>
  arrange(r)|>
  print(n = 13)

config_pivot|>
  mutate(r = instr_inner / instr_hints - 1)|>
  select(data_name, op, instr_hints, instr_inner, r)|>
  arrange(r)

config_pivot|>
  mutate(r = L1_miss_inner / L1_miss_hints - 1)|>
  select(data_name, op, L1_miss_hints, L1_miss_inner, r)|>
  arrange(r)|>
  print(n = 50)

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
      summarise(txs = median(txs) / 1e6)|>
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
WH_CONFIGS <- c('baseline' = 2, 'dense3' = 2, 'hash' = 2, 'wh' = 3)
ALL_CONFIGS <- c(HOT_ART_CONFIGS, 'wh' = 4, 'tlx' = 5)

in_mem_plot(COMMON_OPS, c('baseline' = 2, 'dense3' = 2, 'hash' = 2, 'art' = 3, 'hot' = 3, 'wh' = 1, tlx = 4))
save_as('tmp', 90)
in_mem_plot('ycsb_c_init')
in_mem_plot('ycsb_e')

config_pivot|>
  transmute(r = txs_art / txs_dense3, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 25)
config_pivot|>
  transmute(r = txs_hot / txs_art, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 25)
config_pivot|>
  transmute(r = txs_adapt2 / txs_art, op, data_name)|>
  arrange(op, data_name)

config_pivot|>
  filter(op == 'scan')|>
  transmute(r = txs_baseline / pmax(txs_art, txs_hot), r3 = txs_adapt2 / txs_baseline, r4 = txs_adapt2 / pmax(txs_art, txs_hot), op, data_name)|>
  arrange(op, data_name)

config_pivot|>
  filter(op %in% COMMON_OPS)|>
  mutate(ra = txs_adapt2, rb = txs_baseline)|>
  select(ra, rb, op, data_name, contains('txs_'))|>
  pivot_longer(contains('txs_'), names_prefix = 'txs_')|>
  filter(name %in% c('hot', 'art', 'tlx', 'wh', 'baseline'))|>
  mutate(vb = value / rb - 1, va = value / ra - 1, rvb = rb / value - 1, rva = ra / value - 1, op, name, data_name, .keep = 'none')|>
  arrange(name, op, data_name)|>
  View()


config_pivot|>
  transmute(r1 = txs_baseline / txs_tlx, r2 = pmax(txs_baseline, txs_dense3, txs_hash) / txs_tlx, r3 = txs_baseline / txs_tlx - 1, op, data_name)|>
  arrange(data_name, op)|>View()

config_pivot|>
  transmute(r = txs_wh / pmax(txs_dense3, txs_hash) - 1, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 50)

config_pivot|>
  transmute(r = pmax(txs_dense3, txs_hash) / txs_wh - 1, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 50)

config_pivot|>
  transmute(r = txs_hash / txs_wh - 1, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 50)

config_pivot|>
  transmute(r = txs_dense3 / txs_wh - 1, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 50)

config_pivot|>
  transmute(r = txs_adapt2/txs_lits - 1, op, data_name)|>
  arrange(op, data_name)|>
  print(n = 50)


config_pivot|>
  filter(op == 'insert90')|>
  select(txs_wh, data_name)|>
  pivot_wider(names_from = 'data_name', values_from = 'txs_wh')|>
  mutate(x = ints / sparse)

{
  colors <- c(
    brewer_pal(palette = 'RdBu')(8)[c(6, 8)],
    brewer_pal(palette = 'Dark2')(6)[c(2,4,6,5,1)]
  )

  f <- function(data_filter, art)
    d|>
      filter(config_name %in% c('baseline', 'adapt2', 'art', 'hot', 'tlx', 'wh','lits'), op %in% c('ycsb_c', 'insert90', 'scan'))|>
      filter(data_name %in% data_filter)|>
      ggplot() +
      theme_bw() +
      facet_nested(. ~ data_name + op, scales = 'free', labeller = labeller(
        op = OP_LABELS,
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
      (if (art) { theme(axis.title.y = element_blank()) }else { theme(axis.title.y = element_text(size = 8)) }) +
      scale_color_manual(values = colors) +
      scale_fill_manual(values = colors) +
      geom_point(aes(fill = config_name, col = config_name), x = 0, y = -1, size = 0) +
      labs(x = NULL, y = 'Mops/s', fill = 'Worload', col = 'Workload') +
      guides(col = 'none', fill = 'none') +
      geom_bar(aes(config_name, txs / 1e6, fill = config_name), stat = 'summary', fun = median) +
      scale_y_continuous(breaks = (0:10) * if (art) { 3 }else { 1 }, expand = expansion(mult = c(0, 0.05))) +
      scale_x_manual(values = (1:8), labels = c('Base', 'Adapt', 'ART', 'HOT', 'TLX', 'WH','LITS')) +
      coord_cartesian(xlim = c(1, 7))

  (f(c('urls', 'wiki'), FALSE) |
    plot_spacer() |
    f(c('ints', 'sparse'), TRUE)) +
    plot_layout(guides = 'collect', widths = c(1, 0.01, 1)) &
    theme(legend.position = 'bottom', plot.margin = margin(0, 0, 0, 2))
}
save_as('mem-txs', 35, w = 180)

# adapt

#relative
config_pivot|>
  filter(op %in% COMMON_OPS)|>
  mutate(
    #ref_txs_best = pmax(txs_hash, txs_hints, txs_dense3),
    #ref_txs_worst = pmin(txs_hash, txs_hints, txs_dense3),
    ref_txs_hash = txs_hash,
    ref_txs_dense3 = txs_dense3,
    # ref_txs_hints = txs_hints,
  )|>
  pivot_longer(contains('ref_txs_'), names_to = 'reference_name', values_to = 'reference_value', names_prefix = 'ref_txs_')|>
  #transmute(op,data_name,r=txs_adapt2/reference_value-1,reference_name)|>arrange(reference_name,op,r)|>View()
  ggplot() +
  theme_bw() +
  facet_nested(reference_name ~ data_name, scales = 'free', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    reference_name = c('dense3' = 'vs Dense', 'hash' = 'vs FP'),
  )) +
  scale_y_continuous(labels = label_percent(suffix = ''),
                      expand = expansion(mult = 0.1),
                     breaks = function(x)(-10:10) * ifelse(max(x) > 2, 1, 0.2)
  ) +
  scale_x_discrete(labels = OP_LABELS, expand = expansion(add = 0.1)) +
  coord_cartesian(xlim = c(0.4, 3.6)) +
  theme(
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank(),
    legend.margin = margin(0, 0, 0, 0),
    strip.text = element_text(size = 7.5, margin = margin(2, 1, 2, 1)),
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8, hjust = 0.5),
    axis.title.x = element_text(size = 8),
    panel.spacing.x = unit(0.5, "mm"),
    panel.spacing.y = unit(0.5, "mm"),
    legend.position = 'right',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.box.margin = margin(0,0,0,0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 0, 0, 0),
  ) +
  scale_fill_brewer(palette = 'Dark2', labels = OP_LABELS) +
  scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
  geom_point(aes(fill = op, col = op), x = 0, y = -1, size = 0) +
  labs(x = NULL, y = 'op/s Increase (%)', fill = 'Worload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
  geom_col(aes(x = op, fill = op, y = txs_adapt2 / reference_value - 1)) +
  geom_hline(yintercept = 0) +
  expand_limits(y = 0.06)
save_as('adapt-perf', 30)

# absolute
config_pivot|>
  filter(op %in% COMMON_OPS)|>
  select(op, data_name, txs_adapt2, txs_hash, txs_dense3)|>
  pivot_longer(contains('txs_'), names_prefix = 'txs_')|>
  mutate(config_name = factor(name, levels = CONFIG_NAMES))|>
  #transmute(op,data_name,r=txs_adapt2/reference_value-1,reference_name)|>arrange(reference_name,op,r)|>View()
  ggplot() +
  theme_bw() +
  facet_nested(op ~ data_name, scales = 'free', independent = 'y', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    reference_name = CONFIG_LABELS,
  )) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1)), breaks = function(x)(0:10) * ifelse(max(x) > 6, 2, 1)) +
  scale_x_manual(values = c(1, 3, 2), labels = c('adapt2' = 'Adapt', 'hash' = 'FP', 'dense3' = 'Dense'), expand = expansion(add = 0.1)) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_text(size = 8, angle = 30, hjust = 1), ,
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8, hjust = 0.5),
    axis.title.x = element_text(size = 8),
    panel.spacing.x = unit(2, "mm"),
    #axis.ticks.x = element_blank(),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 8, 0, 0),
  ) +
  scale_fill_manual(values = brewer_pal(palette = 'Dark2')(3)[c(1, 3, 2)]) +
  labs(x = NULL, y = 'Mop/s', fill = 'Worload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
  geom_col(aes(x = config_name, fill = config_name, y = value / 1e6)) +
  expand_limits(y = 2)
save_as('adapt-perf', 50)

#relative 2
config_pivot|>
  filter(op %in% COMMON_OPS)|>
  select(op, data_name, txs_adapt2, txs_hash, txs_dense3)|>
  mutate(rr_hash = txs_adapt2 / txs_hash, rr_dense3 = txs_adapt2 / txs_dense3)|>
  pivot_longer(contains('rr_'), names_prefix = 'rr_')|>
  mutate(config_name = factor(name, levels = CONFIG_NAMES))|>
  #transmute(op,data_name,r=txs_adapt2/reference_value-1,reference_name)|>arrange(reference_name,op,r)|>View()
  ggplot() +
  theme_bw() +
  facet_nested(op ~ data_name, scales = 'free', independent = 'y', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    reference_name = CONFIG_LABELS,
  )) +
  scale_y_continuous(expand = expansion(mult = 0.1), labels = label_percent(suffix = '', style_positive = 'plus')) +
  scale_x_manual(values = c(1, 3, 2), labels = c('adapt2' = 'Adapt', 'hash' = 'FP', 'dense3' = 'Dense'), expand = expansion(add = 0.1)) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_text(size = 8, angle = 30, hjust = 1), ,
    #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
    axis.text.y = element_text(size = 8),
    axis.title.y = element_text(size = 8, hjust = 0.5),
    axis.title.x = element_text(size = 8),
    panel.spacing.x = unit(2, "mm"),
    #axis.ticks.x = element_blank(),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.box.margin = margin(0),
    legend.spacing.x = unit(0, "mm"),
    legend.spacing.y = unit(-5, "mm"),
    plot.margin = margin(0, 8, 0, 0),
  ) +
  scale_fill_brewer(palette = 'Dark2') +
  labs(x = 'reference configuration', y = 'op/s advantage of adapt', fill = 'Worload', col = 'Workload') +
  guides(col = guide_legend(override.aes = list(size = 3)), fill = 'none') +
  geom_col(aes(x = config_name, fill = config_name, y = value - 1)) +
  expand_limits(y = c(-0.05, 0.05)) +
  geom_hline(yintercept = 0)
save_as('adapt-perf', 50)

d|>
  filter(run_id < 5)|>
  filter(config_name == 'adapt2')|>
  filter(op %in% c('ycsb_c', 'scan'))|>
  pivot_longer(contains('nodeCount_'), names_to = 'node_type')|>
  filter(value > 0)|>
  filter(node_type != 'nodeCount_Inner')|>
  ggplot() +
  theme_bw() +
  facet_nested(. ~ op, scales = 'free', labeller = labeller(
    op = OP_LABELS,
    data_name = DATA_LABELS,
    reference_name = CONFIG_LABELS,
  )) +
  scale_x_discrete(labels = DATA_LABELS, name = NULL, limits = rev) +
  scale_y_continuous(name = NULL, labels = NULL) +
  geom_bar_pattern(
    aes(x = data_name, y = value / leaf_count, fill = node_type, pattern = node_type),
    pattern_key_scale_factor = 0.3,
    pattern_spacing = 0.2,
    pattern_scale = 0.9,
    pattern_size = 0.01,
    pattern_fill = '#000000',
    pattern_fill2 = '#000000',
    stat = 'summary',
    fun = mean
  ) +
  scale_fill_manual(breaks = c('nodeCount_Hash', 'nodeCount_Leaf', 'nodeCount_Dense'), labels = c('Fingerprinting', 'Comparison', 'Dense'), values = brewer.pal(3, "Dark2")) +
  scale_pattern_manual(breaks = c('nodeCount_Hash', 'nodeCount_Leaf', 'nodeCount_Dense'), labels = c('Fingerprinting', 'Comparison', 'Dense'), values = c('wave', 'none', 'stripe')) +
  coord_flip() +
  theme(legend.position = 'right', legend.title = element_blank(), legend.margin = margin(-15, 0, 0, -5), plot.margin = margin(0))
save_as('adapt_leaf_ratios', 20)

config_pivot|>
  filter(op %in% COMMON_OPS)|>
  group_by(op, data_name)|>
  mutate(op, data_name, r = txs_adapt2 / pmax(txs_hash, txs_dense3) - 1, .keep = 'used')|>
  arrange(r)

d|>
  filter(op == 'ycsb_e', config_name == 'adapt2', data_name == 'wiki')|>
  mutate(l = mean(leaf_count), keys = mean(counted_final_key_count), .keep = 'none')

config_pivot|>
  filter(op %in% COMMON_OPS)|>
  group_by(op, data_name)|>
  mutate(op, data_name, r = txs_adapt2 / txs_baseline, .keep = 'used')|>
  arrange(r, .by_group = TRUE)


{

  relabel <- function(d) mutate(
    d,
    config_name = config_name|>
      fct_relevel('baseline', 'adapt2', 'hot', 'wh', 'art')|>
      fct_recode(
        'B-Tree' = 'baseline',
        'ART' = 'art',
        'HOT' = 'hot',
        'Wormhole' = 'wh',
        'opt. B-Tree' = 'adapt2'
      ),
    op = fct_recode(op,
                    'lookup' = 'ycsb_c',
                    'scan' = 'scan',
                    'insert' = 'insert90'
    ),
    data_name = fct_recode(data_name,
                           'url' = 'urls',
                           'Wiki title' = 'wiki',
                           'sparse int' = 'sparse',
                           'dense int' = 'ints',
    )
  )

  vs_mem <- config_pivot|>
    filter(
      op == 'ycsb_c',
      data_name %in% c('urls', 'ints', 'sparse')
    )|>
    mutate(op, data_name, txs_wh, txs_adapt2, txs_baseline, mem = txs_wh, .keep = 'none')|>
    pivot_longer(contains('txs_'), names_prefix = 'txs_', values_to = 'txs')|>
    mutate(config_name = factor(name, levels = names(CONFIG_LABELS)))|>
    relabel()
  # mem_col <- brewer_pal(palette = 'RdBu')(8)[2] # HOT
  mem_col <- brewer_pal(palette = 'PiYG')(8)[7] # WH
  vs_mem|>
    ggplot() +
    theme_bw() +
    facet_wrap(~data_name, nrow = 1) +
    theme(
      axis.text.x = element_text(angle = 20, hjust = 1, size = 8),
      axis.title.y = element_text(size = 8),
      #axis.ticks.x = element_blank(),
      strip.text = element_text(size = 8, margin = margin(2, 2, 2, 2)),
      legend.text = element_text(margin = margin(t = 0)),
      legend.title = element_blank(),
      legend.box.margin = margin(0),
      legend.spacing.x = unit(1, "mm"),
      legend.position = 'bottom',
      legend.margin = margin(-10, 0, 0, -20),
      plot.margin = margin(0, 0, 0, 1),
      legend.key.size = unit(4, 'mm'),
      panel.grid.major.x = element_blank(),
    ) +
    scale_color_manual(values = c(brewer_pal(palette = 'RdBu')(8)[c(6, 8)], mem_col)) +
    scale_fill_manual(values = c(brewer_pal(palette = 'RdBu')(8)[c(6, 8)], mem_col)) +
    #geom_point(aes(fill = config_name, col = config_name), x = 0, y = -1, size = 0) +
    labs(x = NULL, y = 'Mlookup/s', fill = 'Worload', col = 'Workload') +
    guides(fill = 'none', #guide_legend(override.aes = list(size = 3),drop = FALSE)
           col = 'none', alpha = 'none') +
    geom_col(aes(config_name, txs / 1e6, fill = config_name)) +
    geom_hline(data = vs_mem, aes(yintercept = mem / 1e6), col = mem_col, linetype = 'dashed', size = 0.4) +
    #geom_linerange(data=vs_mem,aes(x=data_name, ymin=pmin(txs,mem)/1e6, ymax=pmax(txs,mem)/1e6, group = config_name),position=position_dodge(width=0.9),col=brewer_pal(palette = 'Dark2')(3)[3],alpha=0.5,linetype='dashed')+
    geom_segment(
      data = vs_mem,
      aes(x = config_name, xend = config_name, y = mem / 1e6, yend = txs / 1e6, group = config_name, alpha = config_name),
      col = mem_col,
      arrow = arrow(angle = 30, ends = "last", type = "closed", length = unit(2, 'mm'))
    ) +
    geom_text(
      data = vs_mem,
      aes(
        config_name,
        pmax(txs, mem) / 1e6,
        label = label_percent(suffix = "%", accuracy = 1, style_positive = "plus")(txs / mem - 1),
        alpha = config_name,
      ),
      position = position_dodge(width = 0.9),
      vjust = -0.5,
      size = 3) +
    scale_y_continuous(expand = expansion(c(0, 0)), breaks = (0:6) * 2, limits = c(0, 7.9)) +
    scale_alpha_manual(values = c(1, 1, 0)) +
    coord_cartesian(xlim = c(1, 3))

  #scale_x_manual(values = c(0.5, 1.5, 2.5, 3.5))
}
save_as('title', 30)

config_pivot|>
  mutate(r = txs_baseline / txs_hot, data_name, op, .keep = 'none')|>
  arrange(r)|>
  print(n = 50)
config_pivot|>
  mutate(r = txs_adapt2 / txs_hot, data_name, op, .keep = 'none')|>
  arrange(r)|>
  print(n = 50)

# dense
config_pivot|>
  mutate(r3 = txs_dense3 / txs_hints, r2 = txs_dense2 / txs_hints, data_name, op, .keep = 'none')|>
  filter(!data_name %in% c('ints', 'partitioned_id'))|>
  print(n = 50)

config_pivot|>
  select(op, data_name, txs_adapt2, data_size)|>
  filter(op %in% COMMON_OPS)
d|>
  filter(config_name == 'adapt2')|>
  filter(op == 'scan')|>
  filter(run_id == 1)|>
  arrange(data_name)|>
  glimpse()

#adaptive paramter tuning
adapt <-
  d|>
    filter(config_name %in% c('hash', 'hints'), data_name %in% c('wiki', 'urls'), op %in% c('ycsb_c', 'ycsb_e'))|>
    group_by(config_name, data_name, op)|>
    summarize(rtx = median(time / scale))|>
    pivot_wider(names_from = 'config_name', values_from = rtx)|>
    mutate(hash_advantage = hints - hash, .keep = 'unused')|>
    pivot_wider(names_from = 'op',values_from = 'hash_advantage')|>
    mutate(ratio = -ycsb_e/ycsb_c)

{
  lits_train_size <-d|>
    filter(config_name %in% c('lits','lits2'))|>
    group_by(op,config_name,data_name)|>
    summarize(txs=median(txs))|>
    pivot_wider(names_from = 'config_name',values_from = 'txs')

  lits_train_size|>mutate(change = lits2/lits-1)|>
    arrange(change)

  lits_train_size|>transmute(ratio = lits2/lits)|>pull()|>log()|>mean()|>exp()-1

  lits_train_size|>transmute(ratio = lits2/lits)|>pull()|>reciprocal()|>mean()|>reciprocal()
}



