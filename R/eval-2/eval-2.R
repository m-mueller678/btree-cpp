source('../common.R')


r <- bind_rows(
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-2.csv
  read_broken_csv('seq-zipf-2.csv')|>
    filter(!(config_name == 'hot' & (data_name %in% c('int','rng4')))),
  # hot build with integer specialization
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 3 5 :::  'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hot-n3-ycsb | tee R/eval-2/seq-zipf-hot.csv
  read_broken_csv('seq-zipf-hot.csv'),
  # sorted scans for hash vs hint
  # init is wrongly labeled as ycsb_c_init
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 50) ::: 501 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hints-n3-ycsb named-build/hash-n3-ycsb | tee R/eval-2/sorted-scan-seq.csv
  read_broken_csv('sorted-scan-seq.csv')|>filter(op == 'sorted_scan'),
  read_broken_csv('dense-sorted.csv'),
)

COMMON_OPS <- c("ycsb_c", "ycsb_c_init", "ycsb_e")

d <- r |>
  filter(op != "ycsb_e_init")|>
  augment()|>
  filter(scale > 0)

config_pivot <- d|>
  filter(!(config_name %in% c('adapt', 'hot', 'art', 'tlx')))|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = OUTPUT_COLS, values_fn = mean)|>
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


d|>
  filter(!(config_name %in% c('adapt', 'hot', 'art', 'tlx')))|>
  filter(op %in% c('ycsb_c', 'ycsb_c_init'))|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free',labeller = labeller(
    op=OP_LABELS,
    data_name=DATA_LABELS,
  ))+
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('op/s'))
  ) +
  scale_fill_brewer(labels = CONFIG_LABELS,type='qual',palette='Dark2') +
  theme_bw()+
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank()) +
  labs(x = NULL, y = "Throughput", fill = 'Configuration')+
  theme(
    text = element_text(size = 24),
        legend.position = 'bottom',
        panel.spacing.y = unit(1.5, "lines"),
  )+
  guides(fill = guide_legend(ncol = 2))

d|>
  filter(!(config_name %in% c('adapt', 'hot', 'art', 'tlx')))|>
  filter(op %in% c('ycsb_c', 'ycsb_c_init'))|>
  display_rename()|>
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
  mutate(r=txs_hints/txs_baseline)|>
  select(data_name,op,txs_baseline,txs_hints,r)|>
    arrange(r)

config_pivot|>
  select(data_name, op, txs_baseline, txs_best, txs_best_speedup)|>
  arrange(op, txs_best_speedup)

config_pivot|>
  select(data_name, op, node_count_baseline, node_count_best, best_space_saving)|>
  arrange(best_space_saving)


# prefix

config_pivot|>
  mutate(r=keys_per_leaf_prefix/keys_per_leaf_baseline)|>
  select(data_name,op,keys_per_leaf_prefix,keys_per_leaf_baseline,r)|>
  arrange(r)

config_pivot|>
  mutate(r=1-node_count_prefix/node_count_baseline)|>
  select(data_name,op,node_count_prefix,node_count_baseline,r)|>
  arrange(r)

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

config_pivot|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = txs_prefix / txs_baseline - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

config_pivot|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = LLC_miss_prefix / LLC_miss_baseline - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "change in L3-misses")


config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = 1 - node_count_prefix / node_count_baseline)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  labs(x = 'key set', y = "Space Savings") +
  coord_flip()

#heads
config_pivot|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = txs_heads / txs_prefix - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

(
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    geom_col(aes(x = data_name, y = node_count_heads / node_count_prefix - 1)) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
    labs(x = 'key set', y = "Relative") +
    coord_flip()
) | (
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    geom_col(aes(x = data_name, y = node_count_heads * 4096 / final_key_count_heads - node_count_prefix * 4096 / final_key_count_prefix)) +
    scale_y_continuous(
      labels = function(y) paste0("+", y, " B"),
      expand = expansion(mult = c(0, .1))) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank()) +
    coord_flip()
)

config_pivot|>
  mutate(r=1-keys_per_leaf_heads/keys_per_leaf_prefix)|>
  select(data_name,op,keys_per_leaf_heads,keys_per_leaf_prefix,r)|>
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

#hints

config_pivot|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = txs_hints / txs_heads - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = node_count_hints / node_count_heads - 1)) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  labs(x = 'key set', y = "Space Overhead") +
  coord_flip()

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
  arrange(r)

config_pivot|>
  mutate(r = LLC_miss_hints / LLC_miss_heads - 1)|>
  select(data_name, op, LLC_miss_heads, LLC_miss_hints, r)|>
  arrange(r)

config_pivot|>
  mutate(r = br_miss_hints / br_miss_heads - 1)|>
  select(data_name, op, br_miss_heads, br_miss_hints, r)|>
  arrange(r)

# hash

config_pivot|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = txs_hash / txs_hints - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

(
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    geom_col(aes(x = data_name, y = 1 - node_count_hash / node_count_hints)) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
    labs(x = 'key set', y = "Relative") +
    coord_flip()
) | (
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    geom_col(aes(x = data_name, y = node_count_hints * 4096 / final_key_count_hints - node_count_hash * 4096 / final_key_count_hash)) +
    scale_y_continuous(
      labels = function(y) paste0(y, " B"),
      expand = expansion(mult = c(0, .1))) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank()) +
    coord_flip()
)

config_pivot|>
  mutate(r = txs_hash / txs_hints - 1)|>
  select(data_name, op, txs_hints, txs_hash, r)|>
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

#dense leaves

dense_joined <- d|>
  filter(config_name %in% c('dense1', 'dense2'))|>
  full_join(d|>filter(config_name == 'hints'), by = c('op', 'run_id', 'data_name'), relationship = 'many-to-one')

config_pivot|>
  mutate(r = txs_dense1 / txs_hints - 1)|>
  select(data_name, op, txs_hints, txs_dense1, r)|>
  arrange(r)

config_pivot|>
  mutate(r = txs_dense2 / txs_hints - 1)|>
  select(data_name, op, txs_hints, txs_dense2, r)|>
  arrange(r)

d|>
  group_by(config_name, data_name, op)|>
  mutate(count = n())|>
  filter(n() > 1)|>
  glimpse()
d|>
  filter(config_name == 'art', data_name == 'urls', op == 'ycsb_c')|>
  glimpse()

config_pivot|>
  filter(data_name == 'ints')|>
  select(data_name, op, br_miss_hints, br_miss_dense1, br_miss_dense2)

config_pivot|>
  filter(data_name == 'ints')|>
  filter(op == 'ycsb_c')|>
  select(data_name, op, node_count_hints, node_count_dense1, node_count_dense2,
         final_key_count_hints, final_key_count_dense1, final_key_count_dense2)|>
  glimpse()

# TODO dense2 final key count seems broken.

d|>
  filter(config_name == 'dense2', data_name == 'ints')|>
  View()

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
    geom_bar(aes(x = config_name.x, y = node_count.x * 4096 / final_key_count.x - node_count.y * 4096 / final_key_count.y), stat = 'summary', fun = mean) +
    scale_y_continuous(
      labels = function(y) paste0(y, " B"),
      expand = expansion(mult = c(0, .1))) +
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank()) +
    coord_flip()
)

d|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e", "sorted_insert"))|>
  filter(config_name %in% c("dense1", "dense2", "hints"))|>
  filter(data_name == 'ints')|>
  group_by(op, config_name)|>
  summarise(txs = mean(txs))


d|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e", "sorted_insert"))|>
  filter(config_name %in% c("dense1", "dense2", "hints"))|>
  filter(data_name == 'ints')|>
  ggplot() +
  geom_bar(aes(x = op, fill = config_name, y = txs), stat = 'summary', fun = mean, position = 'dodge')

d|>
  filter(op == 'sorted_insert', config_name == 'dense2')|>
  glimpse()

dense_joined|>
  filter(data_name == 'ints')|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e", "sorted_insert"))|>
  ggplot() +
  facet_nested(. ~ config_name.x, labeller = labeller('config_name.x' = CONFIG_LABELS)) +
  geom_bar(aes(x = op, y = txs.x / txs.y - 1), position = 'dodge', stat = 'summary', fun = mean) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

var_density <- read_broken_csv('dense-density.csv')|>
  filter(op %in% c("ycsb_c"), run_id <= 15)|>
  augment()

(
  var_density|>
    ggplot(aes(x = density, y = txs, col = config_name)) +
    geom_smooth(se = FALSE) +
    scale_x_continuous(labels = NULL, expand = expansion(add = 0)) +
    scale_y_continuous(
      expand = expansion(mult = c(0, .1)),
      labels = label_number(scale_cut = cut_si('tx/s'))
    ) +
    geom_point() +
    labs(y = 'Throughput', x = NULL) +
    theme(axis.title.y = element_text(angle = 0, vjust = 0.5, hjust = 1), legend.position = "none")
) /
  (
    var_density|>
      ggplot(aes(x = density, y = node_count * 4096 / 25000000, col = config_name)) +
      geom_point() +
      labs(y = "Space per Record", x = NULL) +
      scale_x_continuous(expand = expansion(add = 0), labels = NULL) +
      scale_y_continuous(
        labels = function(y) paste0(y, " B"),
        expand = expansion(mult = c(0, .1))) +
      theme(axis.title.y = element_text(angle = 0, vjust = 0.5, hjust = 1))
  ) /
  (
    var_density|>
      ggplot(aes(x = density, y = (nodeCount_Dense2 + nodeCount_Dense) / leaf_count, col = config_name)) +
      geom_point() +
      scale_y_continuous(labels = label_percent()) +
      scale_x_continuous(expand = expansion(add = 0), labels = label_percent()) +
      labs(y = "Fraction of Dense Leaves") +
      theme(axis.title.y = element_text(angle = 0, vjust = 0.5, hjust = 1), legend.position = "none")
  )


#integer separators
config_pivot|>
  filter(op %in% COMMON_OPS)|>
  ggplot() +
  facet_nested(. ~ data_name, independent = 'y', scales = 'free') +
  geom_col(aes(x = op, y = txs_inner / txs_hints - 1)) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_discrete(labels = OP_LABELS) +
  labs(x = NULL, y = "Speedup")

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

in_mem_plot <-function(show_op){
  d|>
    filter(config_name %in% c('baseline','art', 'hot', 'dense1', 'hash'), op ==show_op)|>
    ggplot() +
    facet_nested(. ~ data_name, scales = 'free') +
    geom_bar(aes(x = config_name, y = txs, fill = config_name), stat = 'summary', fun = mean) +
    scale_x_discrete(labels = CONFIG_LABELS) +
    scale_y_continuous(
      expand = expansion(mult = c(0, .1)),
      labels = label_number(scale_cut = cut_si('tx/s'))
    ) +
    scale_fill_brewer(palette = "Dark2") +
    labs(x = NULL, y = "Throughput", fill = 'Configuration') +
    theme(axis.text.x = element_blank())
}

in_mem_plot('ycsb_c')
in_mem_plot('ycsb_c_init')
in_mem_plot('ycsb_e')

d|>
  filter(config_name %in% c('baseline','art', 'hot', 'dense1', 'hash'),op %in% COMMON_OPS)|>
  ggplot() +
  facet_nested(op~data_name, scales = 'free',independent = 'y') +
  geom_bar(aes(x = config_name, y = txs, fill = config_name), stat = 'summary', fun = mean) +
  scale_x_discrete(labels = CONFIG_LABELS) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_brewer(palette = "Dark2") +
  labs(x = NULL, y = "Throughput", fill = 'Configuration') +
  theme_bw()+theme(axis.text.x = element_blank(),legend.position = "bottom", legend.justification = "center")

d|>
  filter(config_name %in% c('baseline','art', 'hot', 'dense1', 'hash'),op %in% COMMON_OPS)|>
  ggplot() +
  facet_wrap(~ op+data_name, scales = 'free') +
  geom_bar(aes(x = config_name, y = LLC_miss, fill = config_name), stat = 'summary', fun = mean) +
  scale_x_discrete(labels = CONFIG_LABELS) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_brewer(palette = "Dark2") +
  labs(x = NULL, y = "Count", fill = 'Configuration') +
  theme(axis.text.x = element_blank())
