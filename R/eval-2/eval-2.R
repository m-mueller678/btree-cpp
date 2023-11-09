source('../common.R')


r <- bind_rows(
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 10) ::: 3 5 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/eval-2/seq-zipf-2.csv
  read_broken_csv('seq-zipf-2.csv'),
  # sorted scans for hash vs hint
  # parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 {4} ::: $(seq 1 10) ::: 501 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/hints-n3-ycsb named-build/hash-n3-ycsb | tee R/eval-2/sorted-scan-seq.csv
  read_broken_csv('sorted-scan-seq.csv')
)


d <- r |>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e","sorted_scan"))|>
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
  display_rename()|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free') +
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_y_continuous(
    expand = expansion(mult = c(0, .1)),
    labels = label_number(scale_cut = cut_si('tx/s'))
  ) +
  scale_fill_hue() +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank()) +
  labs(x = NULL, y = "Throughput", fill = 'Configuration')

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
  select(data_name, op, txs_baseline, txs_best, txs_best_speedup)|>
  arrange(op, txs_best_speedup)

config_pivot|>
  select(data_name, op, node_count_baseline, node_count_best, best_space_saving)|>
  arrange(best_space_saving)


# prefix

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
)|(
  config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  geom_col(aes(x = data_name, y = node_count_heads*4096/final_key_count_heads - node_count_prefix*4096/final_key_count_prefix)) +
  scale_y_continuous(
    labels = function(y) paste0("+", y," B"),
                     expand = expansion(mult = c(0, .1)))+
  labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank())+
  coord_flip()
)


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

d|>count(op,data_name,config_name)|>glimpse()

d|>
  filter(config_name %in% c('heads','prefix'))|>
  filter(data_name %in% c('ints','sparse'))|>
  ggplot()+
  facet_nested(config_name~.)+
  geom_freqpoly(aes(x=leaf_count*4096/final_key_count,col=data_name))

d|>
  filter(config_name=='heads')|>
  filter(data_name %in% c('ints','sparse'))|>
  select(data_name,leaf_count)|>
  arrange(data_name,leaf_count)|>View()


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
  mutate(r = instr_hints - instr_heads )|>
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
    geom_col(aes(x = data_name, y = 1-node_count_hash / node_count_hints)) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
    labs(x = 'key set', y = "Relative") +
    coord_flip()
)|(
  config_pivot|>
    filter(op == 'ycsb_c')|>
    ggplot() +
    geom_col(aes(x = data_name, y = node_count_hints*4096/final_key_count_hints - node_count_hash*4096/final_key_count_hash)) +
    scale_y_continuous(
      labels = function(y) paste0(y," B"),
      expand = expansion(mult = c(0, .1)))+
    labs(y = "Per Record") +
    theme(axis.title.y = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank())+
    coord_flip()
)

config_pivot|>
  mutate(r = instr_hash / instr_hints - 1)|>
  select(data_name, op, instr_hints, instr_hash, r)|>
  arrange(r)

config_pivot|>
  mutate(r = instr_hash - instr_hints )|>
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