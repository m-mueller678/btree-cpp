source("../common.R")


# r <- read_broken_csv('vary1.csv.gz')
r <- bind_rows(
  read_broken_csv('v7.csv.gz')
)

d <- r|>
  filter(op != 'ycsb_e_init')|>
  augment()|>
  filter(pmin(psl, psi) >= case_when(data_name == 'urls' ~ 11, data_name == 'wiki' ~ 10, TRUE ~ 8))|>
  filter(payload_size>1)

d_psv<- bind_rows(
  d|>mutate(inner = TRUE),
  d|>mutate(inner = FALSE),
)|>
  filter((inner | psi == 12))|>
  filter(!inner | psl == 12)|>
  mutate(psv = ifelse(inner, psi, psl))

d_psv|>group_by(op,data_name,config_name,inner,psv,payload_size)|>count()|>filter(n!=5)|>View()

  v<-d_psv|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = median)|>
  mutate(txs = txs_adapt2)|>
  select(op, data_name, inner, payload_size, psv, txs, contains('nodeCount'))

v4kb <- v|>
  group_by(op, data_name, inner, payload_size)|>
  mutate(
    ref2_txs = txs[psv == 11],
    ref_txs = txs[psv == 12],
    ref8_txs = txs[psv == 13],
    ref16_txs = txs[psv == 14],
    normalized_txs = txs / ref_txs,
    dense_ratio = nodeCount_Dense_adapt2 / (nodeCount_Dense_adapt2 + nodeCount_Leaf_adapt2),
    .groups = 'drop'
  )

v4kb|>ungroup()|>mutate(r=ref_txs/txs)|>select(op, data_name, inner,psv, payload_size,r)|>filter(data_name!='ints' & payload_size==8)|>summarize(r=min(r))

# size plot
size_plot <- function(pl)
  v4kb|>
    filter(psv >= 10, payload_size == pl)|> #smaller than 1KiB is always worse
    ggplot() +
    theme_bw() +
    facet_nested(inner ~ data_name, scales = 'free',
                 labeller = labeller(
                   inner = function(x)ifelse(x, 'Inner', 'Leaf'),
                   config_name = CONFIG_LABELS,
                   op = OP_LABELS,
                   data_name = DATA_LABELS,
                 )
    ) +
    geom_point(aes(psv, normalized_txs, col = op, shape = op, alpha = op), size = 1) +
    geom_line(aes(psv, normalized_txs, col = op, shape = op), linewidth = 0.3) +
    scale_y_continuous(name = 'op/s relative to 4KiB', breaks = (0:100) * 0.2, expansion(c(0, 0))) +
    scale_x_continuous(breaks = (0:100) * 2, name = "Node Size (KiB)", labels = function(x) 2^(x - 10)) +
    scale_alpha_manual(values = c(0, 1, 1), labels = OP_LABELS) +
    scale_shape_manual(values = c(1, 1, 4), labels = OP_LABELS) +
    scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
    coord_cartesian(ylim = c(0.7, 1.3)) +
    theme(
      strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
      axis.text.x = element_text(size = 7, angle = 90, hjust = 1, vjust = 0.5),
      axis.text.y = element_text(size = 8, vjust = 0.5),
      axis.title = element_text(size = 8),
      panel.spacing = unit(0.5, "mm"),
      plot.margin = margin(0, 0, 0, 0),
      legend.position = 'bottom',
      legend.text = element_text(margin = margin(t = 0)),
      legend.title = element_blank(),
      legend.margin = margin(-10, 0, 0, 0),
      legend.box.margin = margin(0, 0, 0, 0),
      legend.spacing.x = unit(3, "mm"),
    )

size_plot(8)
save_as('node-size-08', 50)
size_plot(16)
size_plot(32)
size_plot(64)


# with two dense
v4kb|>
  filter(psv >= 10, payload_size == 8 | payload_size == 64 & data_name == 'ints')|> #smaller than 1KiB is always worse
  ggplot() +
  theme_bw() +
  facet_nested(inner ~ data_name + payload_size, scales = 'free',
               labeller = labeller(
                 inner = function(x)ifelse(x, 'Inner', 'Leaf'),
                 config_name = CONFIG_LABELS,
                 op = OP_LABELS,
                 data_name = DATA_LABELS,
               )
  ) +
  geom_point(aes(psv, normalized_txs, col = op, shape = op, alpha = op), size = 1) +
  geom_line(aes(psv, normalized_txs, col = op, shape = op), linewidth = 0.3) +
  scale_y_continuous(name = 'op/s relative to 4KiB', breaks = (0:100) * 0.2, expansion(c(0, 0))) +
  scale_x_continuous(breaks = (0:100) * 2, name = "Node Size (KiB)", labels = function(x) 2^(x - 10)) +
  scale_alpha_manual(values = c(0, 1, 1), labels = OP_LABELS) +
  scale_shape_manual(values = c(1, 1, 4), labels = OP_LABELS) +
  scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
  coord_cartesian(ylim = c(0.7, 1.3)) +
  theme(
    strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
    axis.text.x = element_text(size = 7, angle = 90, hjust = 1, vjust = 0.5),
    axis.text.y = element_text(size = 8, vjust = 0.5),
    axis.title = element_text(size = 8),
    panel.spacing = unit(0.5, "mm"),
    plot.margin = margin(0, 0, 0, 0),
    legend.position = 'bottom',
    legend.text = element_text(margin = margin(t = 0)),
    legend.title = element_blank(),
    legend.margin = margin(-10, 0, 0, 0),
    legend.box.margin = margin(0, 0, 0, 0),
    legend.spacing.x = unit(3, "mm"),
  )
save_as('node-size', 50)

# dense-large
{
  tx <- v4kb|>
    filter(data_name == 'ints', !inner)|>
    ggplot() +
    theme_bw() +
    facet_nested(. ~ payload_size, scales = 'free',
                 labeller = labeller(
                   inner = function(x)ifelse(x, 'Inner', 'Leaf'),
                   config_name = CONFIG_LABELS,
                   op = OP_LABELS,
                   data_name = DATA_LABELS,
                 )
    ) +
    geom_point(aes(psv, normalized_txs, col = op, shape = op, alpha = op), size = 1) +
    geom_line(aes(psv, normalized_txs, col = op, shape = op), linewidth = 0.3) +
    scale_y_continuous(name = 'op/s relative to 4KiB', breaks = (0:100) * 0.2, expansion(c(0, 0))) +
    scale_x_continuous(breaks = (0:100) * 2, name = "Node Size (KiB)", labels = function(x) 2^(x - 10)) +
    scale_alpha_manual(values = c(0, 1, 1), labels = OP_LABELS) +
    scale_shape_manual(values = c(1, 1, 4), labels = OP_LABELS) +
    scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
    coord_cartesian(ylim = c(0.7, 1.3)) +
    theme(
      strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
      axis.text.x = element_text(size = 7, angle = 90, hjust = 1, vjust = 0.5),
      axis.text.y = element_text(size = 8, vjust = 0.5),
      axis.title = element_text(size = 8),
      panel.spacing = unit(0.5, "mm"),
      plot.margin = margin(0, 0, 0, 0),
      legend.position = 'bottom',
      legend.text = element_text(margin = margin(t = 0)),
      legend.title = element_blank(),
      legend.margin = margin(-10, 0, 0, 0),
      legend.box.margin = margin(0, 0, 0, 0),
      legend.spacing.x = unit(3, "mm"),
    )
  ratio <-
    v4kb|>
      filter(data_name == 'ints', !inner)|>
      mutate(payload_size = factor(payload_size))|>
      ggplot() +
      theme_bw() +
      geom_point(aes(psv, dense_ratio, col = payload_size, shape = payload_size, alpha = payload_size), size = 2) +
      geom_line(aes(psv, dense_ratio, col = payload_size, shape = payload_size), linewidth = 0.6) +
      scale_y_continuous(name = 'Share of Dense Leaves (%)', labels = label_percent(suffix = ''), breaks = (0:100) * 0.2, expansion(c(0, 0)), position = 'right') +
      scale_x_continuous(breaks = (0:100) * 2, name = "Node Size (KiB)", labels = function(x) 2^(x - 10)) +
      scale_alpha_manual(name = 'Payload Size', values = c(0, 1, 1), labels = OP_LABELS) +
      scale_shape_manual(name = 'Payload Size', values = c(1, 1, 4), labels = OP_LABELS) +
      scale_color_brewer(name = 'Payload Size', palette = 'YlOrRd', labels = OP_LABELS) +
      coord_cartesian(ylim = c(0, 1)) +
      theme(
        strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
        axis.text.x = element_text(size = 7, angle = 90, hjust = 1, vjust = 0.5),
        axis.text.y = element_text(size = 8, vjust = 0.5),
        axis.title = element_text(size = 8),
        panel.spacing = unit(0.5, "mm"),
        plot.margin = margin(0, 0, 0, 0),
        legend.position = 'bottom',
        legend.text = element_text(margin = margin(t = 0)),
        legend.margin = margin(-10, 0, 0, 0),
        legend.box.margin = margin(0, 0, 0, 0),
        legend.spacing.x = unit(3, "mm"),
      )
  (tx | ratio) & theme(
    plot.margin = margin(0, 0, 0, 0),
  )
}
save_as('node-size-dense', 30)


# dense-large
{
  MIN <- 0.5
  MAX <- 1.5

  tx_data <- v4kb|>
    filter(data_name == 'ints', !inner)|>
    mutate(payload_size = factor(payload_size))|>
    filter(psv > 9)
  leaf_data <- tx_data|>
    filter(op == 'ycsb_c')|>
    mutate(op = factor('hack_dense_leaf_share', names(OP_LABELS)))|>
    mutate(normalized_txs = dense_ratio * (MAX - MIN) + MIN)

  bind_rows(tx_data, leaf_data)|>
    ggplot() +
    theme_bw() +
    facet_nested(. ~ payload_size, scales = 'free',
                 labeller = labeller(
                   inner = function(x)ifelse(x, 'Inner', 'Leaf'),
                   config_name = CONFIG_LABELS,
                   op = OP_LABELS,
                   data_name = DATA_LABELS,
                 )
    ) +
    geom_point(aes(psv, normalized_txs, col = op, shape = op, alpha = op), size = 1) +
    geom_line(aes(psv, normalized_txs, col = op, shape = op), linewidth = 0.3) +
    coord_cartesian(ylim = c(MIN, MAX)) +
    scale_y_continuous(name = 'op/s relative to 4KiB (%)',
                       breaks = (0:100) * 0.25,
                       expand = expansion(add = c(0, 0)),
                       labels = label_percent(suffix = ''),
                       sec.axis = sec_axis(
                         trans = ~(. - MIN) / (MAX - MIN),
                         name = "Dense Leaf Share (%)",
                         labels = label_percent(suffix = ''),
                         breaks = (0:100)*0.25
                       )
    ) +
    scale_x_continuous(breaks = (0:100) * 2, name = "Node Size (KiB)", labels = function(x) 2^(x - 10)) +
    scale_alpha_manual(values = c(0, 1, 1, 1), labels = OP_LABELS) +
    scale_shape_manual(values = c(1, 1, 4, 0), labels = OP_LABELS) +
    scale_color_brewer(palette = 'Dark2', labels = OP_LABELS) +
    theme(
      strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
      axis.text.x = element_text(size = 7, angle = 90, hjust = 1, vjust = 0.5),
      axis.text.y = element_text(size = 8, vjust = 0.5),
      axis.title.y.right = element_text(hjust = 0.2),
      axis.title = element_text(size = 8),
      panel.spacing = unit(0.5, "mm"),
      plot.margin = margin(0, 0, 0, 0),
      legend.position = 'bottom',
      legend.text = element_text(margin = margin(t = 0)),
      legend.title = element_blank(),
      legend.margin = margin(-10, 0, 0, 0),
      legend.box.margin = margin(0, 0, 0, 0),
      legend.spacing.x = unit(1, "mm"),
    )
}
save_as('node-size-dense', 30)