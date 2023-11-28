source("../common.R")

# python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee R/eval-dense/var-density.csv
r_var_density <- bind_rows(
  # python3 R/eval-dense/var-density.py |parallel -j1 --joblog joblog -- {1}| tee -a R/eval-dense/var-density.csv
  read_broken_csv('var-density.csv'),
  read_broken_csv('var-density-2.csv')
)

d_var_density <- r_var_density|>augment()


{

  common <- function(dense_only, has_x) d_var_density|>
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
    theme(axis.text.x = if (has_x) { elem_list_text() }else { element_blank() }) +
    guides(col = guide_legend(override.aes = list(size = 1), nrow = 2,title.position="top", title.hjust = 0.5))


  tx <- common(FALSE, FALSE) +
    facet_nested(. ~ op, labeller = labeller(
      op = OP_LABELS,
    )) +
    geom_line(aes(x = density, y = txs, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))
  space <- common(FALSE, TRUE) +
    geom_line(aes(x = density, y = node_count * 4096 / 25e6, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_bytes()) +
    facet_nested(. ~ 'space per record')
  dense <- common(TRUE, TRUE) +
    geom_line(aes(x = density, y = (nodeCount_Dense+nodeCount_Dense2) / leaf_count, col = config_name),stat='summary',fun=mean) +
    scale_y_continuous(name = NULL, labels = label_percent()) +
    facet_nested(. ~ 'dense leaf share')
  (tx / (space | dense)) + plot_layout(guides = "collect") &
    theme(legend.position = 'bottom',plot.margin = margin(1),legend.margin = margin(1))
}
save_as('var-density', 80)