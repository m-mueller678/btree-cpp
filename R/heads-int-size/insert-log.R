source('../common.R')


# parallel --joblog joblog -j4 -- env SEED={1} DATA=int KEY_COUNT=25e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE={2} ZIPF=0.99 DENSITY=1 named-build/{3}-n3-ycsb \| gzip \> R/heads-int-size/insert-log/int-{3}-pl{2}-r{1}.csv.gz ::: $(seq 1 10) ::: 6 8 10 ::: prefix heads
# parallel --joblog joblog -j4 -- env DATA=rng4 KEY_COUNT=4e7 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE={2} ZIPF=0.99 DENSITY=1 named-build/{1}-n3-ycsb \| gzip \> R/heads-int-size/insert-log/sparse-{1}-pl{2}-r{3}.csv.gz ::: prefix heads ::: 6 8 10 ::: 0 1 2

r <- {
  files <- fs::dir_ls(path = 'insert-log/', glob = '*.csv.gz')
  files <- files[grepl('r(1)', files)]
  files <- files[grepl('pl(8)', files)]
  #files <- files[grepl('int-', files)]
  vroom::vroom(files, id = "file", delim = ',', col_names = 'leaves')|>
    group_by(file)|>
    mutate(count = row_number())|>
    filter(count %% 100 == 0)
}
d <- r|>
  extract(file, c('data', 'config', 'pl', 'run_id'), 'insert-log/([[:alnum:]]+)-([[:alnum:]]+)-pl([0-9]+)-r([0-9]+).csv.gz')|>
  mutate(pl = factor(as.integer(pl)))

d|>
  ggplot() +
  facet_nested(config + pl ~ data) +
  geom_line(aes(count, count / leaves, col = run_id)) +
  scale_color_discrete()

d|>filter(pl == 8)|>
  ggplot() +
  facet_nested(run_id ~ data) +
  geom_line(aes(count, count / leaves, col = config)) +
  scale_color_discrete()

d|>
  pivot_wider(names_from = config, values_from = 'leaves')|>
  ggplot() +
  facet_nested(run_id ~ pl) +
  geom_line(aes(count, heads / prefix, col = data)) +
  scale_x_log10() +
  scale_color_discrete() +
  geom_vline(xintercept = 25e6) +
  coord_cartesian(ylim = c(1.2, 1.35), xlim = c(1e5, 1e8))

d|>
  pivot_wider(names_from = config, values_from = 'leaves')|>
  sample_n(1e5)|>
  ggplot() +
  facet_nested(data ~ pl) +
  geom_line(aes(count, (heads - prefix) * 4096 / count, col = run_id)) +
  scale_x_log10() +
  scale_color_discrete() +
  geom_vline(xintercept = 25e6) +
  coord_cartesian(xlim = c(1e5, 1e8))


d|>
  filter(pl == 8, data == 'int')|>
  ggplot() +
  geom_line(aes(count, leaves, col = config)) +
  geom_vline(xintercept = 25e6)

d|>
  filter(pl == 8, data == 'int')|>
  filter(count == 25e6)|>
  arrange(config, leaves)


# parallel --joblog joblog -j4 -- env SEED=1 DATA=int KEY_COUNT={1}e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 named-build/{2}-n3-ycsb \| gzip \> R/heads-int-size/insert-log-var-max/{2}-{1}e6.csv.gz ::: $(seq 1 5) $(seq 10 5 30) ::: prefix heads
rvm <- {
  files <- fs::dir_ls(path = 'insert-log-var-max/', glob = '*.csv.gz')
  files <- files[grepl('25e6', files)]
  vroom::vroom(files, id = "file", delim = ',', col_names = 'leaves')|>
    group_by(file)|>
    mutate(count = row_number())|>
    filter(count %% 100 == 0)
}

dvm <- rvm|>
  extract(file, c('config', 'max_count'), 'insert-log-var-max/([[:alnum:]]+)-([0-9e]+).csv.gz')|>
  mutate(max_count = as.numeric(max_count))|>
  mutate()

dvm|>
  sample_n(1e5)|>
  ggplot() +
  facet_nested(config ~ .) +
  geom_line(aes(count, count / leaves, col = max_count)) +
  scale_color_discrete()

{
  {
    dvm|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(max_count ~ config) +
      geom_line(aes(count / max_count, count / leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(y = 'records per leaf', x = 'inserted integers', col = 'N')
  } |
  {
    dvm|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(max_count ~ config, scales = 'free_y') +
      geom_line(aes(count / max_count, leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(x = 'inserted integers', col = 'N')
  }
} + plot_layout(guides = 'collect')
save_as('int-prefix-space', h = 300, w = 600)

# parallel --joblog joblog -j4 -- env RAND_PL=1 SEED=1 DATA=int KEY_COUNT={1}e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE=17 ZIPF=0.99 DENSITY=1 named-build/{2}-n3-ycsb \| gzip \> R/heads-int-size/insert-log-rand-pl/{2}-{1}e6.csv.gz ::: $(seq 1 10)  ::: prefix heads

rrp <- {
  files <- fs::dir_ls(path = 'insert-log-rand-pl/', glob = '*.csv.gz')
  #files<-files[grepl('r(0|2)',files)]
  vroom::vroom(files, id = "file", delim = ',', col_names = 'leaves')|>
    group_by(file)|>
    mutate(count = row_number())|>
    filter(count %% 100 == 0)
}
drp <- rrp|>
  extract(file, c('config', 'max_count'), 'insert-log-[a-z\\-]+/([[:alnum:]]+)-([0-9e]+).csv.gz')|>
  mutate(max_count = as.numeric(max_count))|>
  mutate()

{
  {
    drp|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(. ~ config) +
      geom_line(aes(count / max_count, count / leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(y = 'records per leaf', x = 'inserted integers', col = 'N')
  } |
  {
    drp|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(max_count ~ config, scales = 'free_y') +
      geom_line(aes(count / max_count, leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(x = 'inserted integers', col = 'N')
  }
} + plot_layout(guides = 'collect')

# parallel --joblog joblog -j12 -- env SEED=1 DATA=int KEY_COUNT={1}e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE=17 ZIPF=0.99 DENSITY=1 named-build/{2}-n3-ycsb \| gzip \> R/heads-int-size/insert-log-median/{2}-{1}e6.csv.gz ::: $(seq 1 10)  ::: prefix heads

rm <- {
  files <- fs::dir_ls(path = 'insert-log-median/', glob = '*.csv.gz')
  #files<-files[grepl('r(0|2)',files)]
  vroom::vroom(files, id = "file", delim = ',', col_names = 'leaves')|>
    group_by(file)|>
    mutate(count = row_number())|>
    filter(count %% 100 == 0)
}
dm <- rm|>
  extract(file, c('config', 'max_count'), 'insert-log-[a-z\\-]+/([[:alnum:]]+)-([0-9e]+).csv.gz')|>
  mutate(max_count = as.numeric(max_count))|>
  mutate()

{
  {
    dm|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(max_count ~ config) +
      geom_line(aes(count / max_count, count / leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(y = 'records per leaf', x = 'inserted integers', col = 'N')
  } |
  {
    dm|>
      sample_n(1e6, replace = TRUE)|>
      ggplot() +
      facet_nested(max_count ~ config, scales = 'free_y') +
      geom_line(aes(count / max_count, leaves, col = factor(max_count))) +
      scale_color_discrete() +
      labs(x = 'inserted integers', col = 'N')
  }
} + plot_layout(guides = 'collect')

# others
# parallel --joblog joblog -j12 -- env SEED=1 DATA=data/{1} KEY_COUNT=3e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 named-build/{2}-n3-ycsb \| gzip \> R/heads-int-size/insert-log-others/{2}-{1}.csv.gz ::: urls-short wiki ::: baseline prefix heads hints hash dense2 dense3 inner adapt
# parallel --joblog joblog -j12 -- env SEED=1 DATA={1} KEY_COUNT=3e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF=0.99 DENSITY=1 named-build/{2}-n3-ycsb \| gzip \> R/heads-int-size/insert-log-others/{2}-{1}.csv.gz ::: int rng4 ::: baseline prefix heads hints hash dense2 dense3 inner adapt

ro <- {
  files <- fs::dir_ls(path = 'insert-log-others/', glob = '*.csv.gz')
  #files<-files[grepl('r(0|2)',files)]
  vroom::vroom(files, id = "file", delim = ',', col_names = 'leaves')|>
    group_by(file)|>
    mutate(count = row_number())|>
    filter(count %% 100 == 0)
}
do <- ro|>
  extract(file, c('config', 'data'), '.*/([[:alnum:]]+)-(.+)\\.csv\\.gz')

do|>
  sample_n(1e6, replace = TRUE)|>
  ggplot() +
  facet_nested(. ~ config) +
  geom_line(aes(count, count / leaves, col = data)) +
  scale_color_discrete()

# final plot
df <- vroom::vroom(
  c(
    'ours-fixed-pl/heads-25e6.csv.gz',
    'ours-fixed-pl/prefix-25e6.csv.gz'
  ), id = "file", delim = ',', col_names = 'leaves')|>
  group_by(file)|>
  mutate(count = row_number())|>
  filter(count %% 100 == 0)|>
  extract(file, c('config', 'max_count'), '.*/([[:alnum:]]+)-([0-9e]+).csv.gz')|>
  mutate(max_count = as.numeric(max_count))|>
  mutate()


separator_misery <- df|>
  sample_n(1e5, replace = TRUE)|>
  mutate(
    records_per_leaf = count / leaves,
  )|>
  pivot_longer(c('records_per_leaf', 'leaves'), names_to = 'metric')|>
  filter(metric == 'records_per_leaf')|>
  ggplot() +
  theme_bw() +
  geom_line(aes(count, value, col = config)) +
  geom_text(aes(x = count, y = ifelse(config == 'heads', 150, 165), col = config, label = CONFIG_LABELS[config]), data = df|>filter(count == 1.2e7), size = 3) +
  labs(x = 'inserted records', y = NULL) +
  scale_x_continuous(limits = c(0, 25e6), labels = label_number(scale_cut = cut_si("")), expand = expansion(mult = c(0, 0.1)),
                     breaks = (0:5) * 1e7) +
  scale_y_continuous(limit = c(130, 190), breaks = (0:100) * 15, expand = expansion(0), name = 'records/leaf') +
  #scale_linetype_manual(values=c('solid','solid'))+
  scale_color_brewer(name = 'Configuration', palette = 'Dark2', labels = CONFIG_LABELS) +
  guides(linetype = 'none', colour = 'none') +
  theme(
    axis.title.y = element_text(size = 8, hjust = 1),
    axis.title.x = element_text(size = 8)
  )


# x scaled
df|>
  sample_n(1e5, replace = TRUE)|>
  mutate(
    records_per_leaf = count / leaves,
  )|>
  pivot_longer(c('records_per_leaf', 'leaves'), names_to = 'metric')|>
  filter(metric == 'records_per_leaf')|>
  ggplot() +
  theme_bw() +
  geom_line(aes(count * ifelse(config == 'prefix', 0.8, 1), ifelse(config == 'prefix', value * 0.9 - 18, value), col = config)) +
  labs(x = 'inserted records', y = NULL) +
  scale_x_continuous(limits = c(0, 1e7), labels = label_number(scale_cut = cut_si("")), expand = expansion(mult = c(0, 0.1)),
                     breaks = (0:5) * 4e6) +
  scale_y_continuous(limit = c(130, 190), expand = expansion(0)) +
  #scale_linetype_manual(values=c('solid','solid'))+
  scale_color_brewer(name = 'Configuration', palette = 'Dark2', labels = CONFIG_LABELS) +
  theme(legend.position = 'right') +
  guides(linetype = 'none')
