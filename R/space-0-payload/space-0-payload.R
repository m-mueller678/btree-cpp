source('../common.R')

r<-read_broken_csv('space-0-payload.csv')


d <- r |>augment()

config_pivot <- d|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = mean)

d|>ggplot()+
  geom_bar(aes(x=data_name,y=node_count*4096/data_size,fill=config_name),stat='summary',fun=mean,position='dodge')+
  scale_fill_brewer(palette = 'Dark2')


config_pivot|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  theme_bw() +
  geom_col(aes(x = data_name, y = 1 - node_count_prefix / node_count_baseline, fill = data_name)) +
  scale_fill_brewer(palette = 'Dark2', labels = DATA_LABELS, name = 'key set') +
  guides(fill = 'none') +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, .1)),) +
  scale_x_discrete(limits = {
    l <- rev(levels(config_pivot$data_name))
    l[l %in% unique((config_pivot|>filter(!is.na(txs_prefix)))$data_name)]
  },labels = DATA_LABELS) +
  labs(x = 'key set', y = NULL) +
  coord_flip() +
  theme(
    plot.margin = margin(0, 0, 0, 0),
    axis.title.y = element_text(size = 8),
  )
save_as('prefix-space', 15)

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
      coord_flip()+
      theme(strip.text = element_text(size=8,margin = margin(2,2,2,2)))
  }

  hide_y <- theme(
    axis.title.y = element_blank(),
    axis.ticks.y = element_blank(),
    axis.text.y = element_blank(),
  )
  pr <- plot('rel') +
    facet_wrap(~'Relative')+
    scale_y_continuous(
      labels = label_percent(),
      name = NULL,
      expand = expansion(mult = c(0, .1)),
      breaks = (0:2) * 0.1
    )
  pa <- plot('abs') +
    facet_wrap(~'Per Record')+
    scale_y_continuous(
      labels = label_bytes(),
      name = NULL,
      expand = expansion(mult = c(0, .1)),
      breaks = c(0, 2, 4, 6)
    )

  (pr + (pa + hide_y)) + plot_layout(guides = 'collect') & theme(legend.position = 'right', legend.margin = margin(0, 0, 0, 0), plot.margin = margin(0, 0, 0, 0), legend.key.size = unit(4, 'mm'))
}

save_as('hash-head-space-overhead', 20)
