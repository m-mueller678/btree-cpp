source('../common.R')

# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={1} DENSITY=1 {4} ::: $(seq -w 50 1 150 | xargs -I{} echo "scale=2;{}/100" | bc) ::: 3 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/in-memory-skew/seq.csv
# r<-read_broken_csv('seq.csv')

# python3 R/in-memory-skew/skew.py |parallel -j1 --joblog joblog --retries 3 -- {1}| tee R/in-memory-skew/skew.csv
#r <- read_broken_csv('skew.csv')
# python3 R/in-memory-skew/skew.py |parallel -j1 --joblog joblog --retries 3 -- {1}| tee R/in-memory-skew/skew2.csv
# christmas run
r <- read_broken_csv('skew-op2.csv.gz')


d <- r |>
  filter(op == 'ycsb_c')|>
  augment()|>
  mutate(
    round_size = cut(total_size, breaks = c(37e6, 38e6, 74e6, 76e6, 149e6, 151e6, 299e6, 301e6, 599e6, 601e6, 1199e6, 1201e6), labels = c(37.5, NA, 75, NA, 150, NA, 300, NA, 600, NA, 1200))
  )

d|>
  group_by(round_size, data_name, config_name)|>
  filter(run_id < 2)|>
  count()|>
  filter(n != 26 * 2)

cp <- {
  cp <- d|>
    pivot_longer(any_of(OUTPUT_COLS), names_to = 'metric')|>
    pivot_wider(id_cols = !any_of(c('bin_name', 'run_id', 'config_name')), names_from = 'config_name', values_from = 'value', values_fn = median, names_prefix = 'x-')
  configs = unique(d$config_name)
  for (c in configs) {
    cr <- paste0('x-', config_reference(c))
    cb <- paste0('x-', c)
    cp[paste0('r-', c)] <- ifelse(cr != 'x-NA', cp[cb] / cp[cr], NA)
    cp[paste0('a-', c)] <- ifelse(cr != 'x-NA', cp[cb] - cp[cr], NA)
    cp[paste0('rhh-', c)] <- ifelse(cr != 'x-NA', cp[cb] / pmax(pull(cp['x-dense3']), pull(cp['x-hash'])), NA)
  }
  cp|>
    pivot_longer(contains("-"), names_sep = '-', names_to = c('reference', 'config_name'))|>
    mutate(config_name = factor(config_name, levels = CONFIG_NAMES))
}

cp|>
  #filter(reference=='x')|>
  filter(metric %in% c('txs', 'L1_miss', 'node_count'))|>
  #filter(round_size %in% c(37.5,300,1200))|>
  #filter(data_name=='ints')|>
  #filter(config_name!='art')|>
  filter(config_name %in% c('inner'))|>
  ggplot() +
  facet_nested(reference + metric ~ round_size + data_name, scales = 'free_y') +
  geom_point(aes(ycsb_zipf, value, col = config_name)) +
  geom_smooth(aes(ycsb_zipf, value, col = config_name), method = 'lm') +
  scale_color_brewer(palette = 'Dark2')

# with urls
{

  zipf_plot <- function(d, legend, y_axis, y_scale) {
    configs <- c('prefix', 'heads', 'hints', 'hash', 'dense2', 'dense3')
    palette <- brewer_pal(palette = 'Dark2')(length(configs))
    names(palette) <- configs
    d|>
      filter(metric == 'txs', reference == 'r')|>
      filter(round_size == 300)|>
      ggplot(aes(ycsb_zipf, value - 1, col = config_name)) +
      theme_bw() +
      facet_nested(. ~ data_name) +
      scale_color_manual(
        name = "Optimization",
        values = palette,
        drop = FALSE,
        labels = CONFIG_LABELS,
      ) +
      scale_y_continuous(
        labels = if (y_axis) { label_percent(style_positive = "plus") }else { NULL },
        breaks = (-1:2) * y_scale,
        limits = c(-y_scale, y_scale * 3),
        expand = expansion(0),
        name = NULL) +
      scale_x_continuous(
        breaks = (1:2) * 0.5,
        name = NULL,
        expand = expansion(0),
      ) +
      guides(
        col = if (legend) { guide_legend(nrow = 2, title = element_blank()) }else { FALSE },
      ) +
      theme(
        legend.position = 'bottom',
        legend.margin = margin(0, 0, 0, -30),
        plot.margin = margin(0, if (y_axis) { 5 }else { 0 }, 0, 0),
      ) +
      (if (y_axis) {
        theme()
      }else {
        theme(axis.ticks.y = element_blank())
      }) +
      #geom_line(size = 0.2) +
      geom_smooth(size = 0.4, se = FALSE) +
      geom_point(size = 0.15, alpha = 0.5) +
      theme()
  }

  p1 <- cp|>
    filter(data_name == 'urls' & config_name %in% c('prefix', 'heads', 'hints', 'hash'))|>
    zipf_plot(TRUE, TRUE, 0.1)
  p2 <- cp|>
    filter(data_name == 'wiki' & config_name %in% c('prefix', 'heads', 'hints', 'hash'))|>
    zipf_plot(FALSE, TRUE, 0.3)
  p3 <- cp|>
    filter(data_name == 'ints' & config_name %in% c('heads', 'hints', 'hash', 'dense3', 'dense2'))|>
    zipf_plot(FALSE, FALSE, 0.3)

  (p1 + p2 + p3) + plot_layout(guides = 'collect') & theme(
    legend.position = 'bottom',
  )
  save_as('zipf-speedup', 50)
}

# without urls
{
  cp|>
    filter(metric == 'txs', reference == 'r')|>
    filter(round_size == 300)|>
    filter(
      data_name == 'urls' & config_name %in% c('prefix', 'heads', 'hints', 'hash') |
        data_name == 'wiki' & config_name %in% c('prefix', 'heads', 'hints', 'hash') |
        data_name == 'ints' & config_name %in% c('prefix', 'heads', 'hints', 'hash', 'dense3', 'dense2')
    )|>
    ggplot(aes(ycsb_zipf, value - 1, col = config_name)) +
    theme_bw() +
    facet_nested(. ~ data_name) +
    scale_color_brewer(type = 'qual', palette = 'Dark2', labels = CONFIG_LABELS, name = "Optimization") +
    scale_y_continuous(
      labels = label_percent(style_positive = "plus"),
      breaks = (-1:2) * 0.3, limits = c(-0.3, 0.9),
      expand = expansion(0),
      name = NULL) +
    scale_x_continuous(
      breaks = (1:2) * 0.5,
      name = NULL,
      expand = expansion(0),
    ) +
    theme(
      legend.position = 'bottom',
      legend.margin = margin(0, 0, 0, -30),
    ) +
    guides(col = guide_legend(nrow = 2, title = element_blank())) +
    geom_point(size = 0.3) +
    geom_smooth(size = 0.3, se = FALSE)
  save_as('zipf-speedup', 50)
}

# full
cp|>
  filter(reference == 'r')|>
  filter(metric == 'txs')|>
  #filter(round_size %in% c(37.5,300,1200))|>
  #filter(data_name=='ints')|>
  #filter(config_name!='art')|>
  #filter(config_name %in% c(BASIC_OPTS,'hash','dense3','dense2','inner'))|>
  ggplot() +
  facet_nested(
    data_name ~ config_name,
    scales = 'free_y',
    independent = 'y',
  ) +
  #geom_point(aes(ycsb_zipf,value,col=config_name))+
  geom_smooth(aes(ycsb_zipf, value, col = round_size), se = FALSE) +
  scale_color_brewer(palette = 'Dark2')

# HOT stuff

cp|>
  #filter(reference=='a')|>
  filter(metric == 'L1_miss')|>
  #filter(round_size %in% c(37.5,300,1200))|>
  #filter(data_name=='ints')|>
  #filter(config_name!='art')|>
  filter(config_name %in% c('hot'))|>
  ggplot() +
  facet_nested(reference ~ round_size, scales = 'free_y') +
  #geom_point(aes(ycsb_zipf,value,col=config_name))+
  geom_smooth(aes(ycsb_zipf, value, col = data_name), se = FALSE) +
  scale_color_brewer(palette = 'Dark2')

cp|>
  filter(reference == 'x')|>
  filter(metric == 'L1_miss')|>
  #filter(round_size %in% c(37.5,300,1200))|>
  #filter(data_name=='ints')|>
  #filter(config_name!='art')|>
  filter(config_name %in% c('hot', 'hints'))|>
  ggplot() +
  facet_nested(data_name ~ round_size) +
  #geom_point(aes(ycsb_zipf,value,col=config_name))+
  geom_smooth(aes(ycsb_zipf, value, col = config_name), se = FALSE) +
  scale_color_brewer(palette = 'Dark2')

cp|>
  filter(reference == 'x', metric == 'L1_miss')|>
  #filter(data_name=='wiki',config_name=='prefix',round_size==75,ycsb_zipf==0.5)|>glimpse()
  group_by(data_name, config_name, round_size)|>
  arrange(ycsb_zipf, .by_group = TRUE)|>
  summarize(min_miss = first(value), max_miss = last(value), .groups = 'drop')|>
  mutate(reduction = min_miss / max_miss)|>
  group_by(round_size, data_name)|>
  arrange(reduction, .by_group = TRUE)|>
  #slice_head(n=3)|>
  mutate(rank = row_number())|>
  mutate(is_hot = config_name == 'hot')|>
  #pivot_wider(values_from = c('config_name','reduction'),names_from='rank',id_cols = c('data_name','round_size'))|>
  ggplot() +
  facet_nested(data_name ~ round_size) +
  geom_col(aes(rank, reduction, fill = is_hot, col = config_name)) +
  scale_fill_manual(values = c('TRUE' = 'red', 'FALSE' = 'grey')) +
  geom_text(aes(rank, reduction, label = config_name), position = position_stack(vjust = 1.0), angle = 90) +
  scale_color_hue()

d|>
  filter(config_name %in% c('prefix', 'heads', 'hints'))|>
  ggplot() +
  facet_nested(data_name ~ round_size, scales = 'free_y') +
  geom_point(aes(ycsb_zipf, txs, col = config_name)) +
  #geom_smooth(aes(ycsb_zipf,txs,col=config_name),se=FALSE)+
  scale_color_brewer(palette = 'Dark2')


d|>
  filter(op == 'ycsb_c')|>
  ggplot() +
  facet_nested(. ~ data_name, scales = 'free_y') +
  geom_line(aes(y = txs, x = ycsb_zipf, col = config_name))


cp|>
  filter(metric == 'txs', op == 'ycsb_c', reference == 'r')|>
  group_by(config_name, data_name)|>
  summarize(vi = min(value), va = max(value))|>
  print(n = 50)
cp|>
  filter(metric == 'L1_miss', op == 'ycsb_c', reference == 'x', ycsb_zipf %in% c(0.5, 1.5), config_name %in% c('art', 'hints', 'hot', 'tlx'))|>
  select(config_name, data_name, value, ycsb_zipf)|>
  pivot_wider(names_from = ycsb_zipf, values_from = 'value', names_prefix = 'z')|>
  arrange(config_name, data_name)|>
  mutate(r = 1 - z1.5 / z0.5)|>
  group_by(config_name)|>
  summarize(ri = min(r), ra = max(r))

# in-mem
{
  data <- cp|>
    filter(data_name %in% c('urls', 'sparse', 'ints'))|>
    filter(reference == 'rhh', metric == 'txs', config_name %in% c('art', 'hot', 'tlx'))

  labels <- data|>
    group_by(data_name, config_name)|>
    filter(ycsb_zipf < 1.0)|>
    summarize(
      #up=config_name!='tlx',
      max = max(value),
      min = min(value),
    )|>
    mutate(
      y = ifelse(config_name != 'tlx', max, min),
      x = 0.5,
      h = 0,
      v = ifelse(config_name != 'tlx', -0.1, 1.1),
      c = CONFIG_LABELS[config_name],
      config_name,
      .keep = 'none'
    )|>
    ungroup()|>
    add_row(data_name = factor('data/urls-short', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
    add_row(data_name = factor('rng4', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
    add_row(data_name = factor('int', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))


  colors <- c(
    brewer_pal(palette = 'RdBu')(8)[8],
    brewer_pal(palette = 'PuOr')(8)[2],
    brewer_pal(palette = 'RdBu')(8)[2],
    brewer_pal(palette = 'PRGn')(8)[2]
  );
  data|>
    #filter(reference == 'rhh',metric=='txs',config_name %in% c('hash','dense3'))|>
    ggplot(aes(x = ycsb_zipf, y = value, col = config_name)) +
    theme_bw() +
    facet_nested(. ~ data_name, scales = 'free', independent = 'y', labeller = labeller(
      op = OP_LABELS,
      data_name = DATA_LABELS,
    )) +
    facetted_pos_scales(
      y = list(
        scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 1.5), breaks = c(0:10) * 0.5),
        scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 3), breaks = c(0:10)),
        scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 3), breaks = c(0:10))
      )
    ) +
    #geom_smooth(aes(x = ycsb_zipf, y = value, col = config_name),size=0.3) +
    geom_hline(yintercept = 1, col = brewer_pal(palette = 'RdBu')(8)[8]) +
    geom_point(aes(x = ycsb_zipf, y = value, col = config_name, fill = config_name), size = 0.1) +
    geom_text(data = labels, mapping = aes(x = x, y = y, col = config_name, hjust = h, vjust = v, label = c), size = 3) +
    scale_color_manual(values = colors, breaks = factor(c('baseline', 'art', 'hot', 'tlx'), levels = CONFIG_NAMES)) +
    #scale_fill_manual(values = colors) +
    scale_x_continuous(
      breaks = (1:3) * 0.5,
      name = 'Zipf-Parameter',
      expand = expansion(0),
    ) +
    expand_limits(y = c(0, 1.1)) +
    guides(fill = 'none', col = 'none') +
    theme(
      strip.text = element_text(size = 8, margin = margin(2, 1, 2, 1)),
      axis.text.x = element_text(size = 8), ,
      #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
      axis.text.y = element_text(size = 8),
      axis.title.y = element_text(size = 8, hjust = 1),
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
    labs(y = 'Normalized op/s')

}
save_as('zipf-in-mem', 30)


# with all data sets
{

  hplot <- function(left) {
    data_filter <- if (left) { c('urls', 'wiki') }else { c('sparse', 'ints') }

    data <- cp|>
      filter(data_name %in% data_filter)|>
      filter(reference == 'rhh', metric == 'txs', config_name %in% c('art', 'hot', 'tlx'))

    labels <- data|>
      group_by(data_name, config_name)|>
      filter(ycsb_zipf < 1.0)|>
      summarize(
        #up=config_name!='tlx',
        max = max(value),
        min = min(value),
      )|>
      mutate(
        y = ifelse(config_name != 'tlx', max, min),
        x = 0.5,
        h = 0,
        v = ifelse(config_name != 'tlx', -0.1, 1.1),
        c = CONFIG_LABELS[config_name],
        config_name,
        .keep = 'none'
      )|>
      ungroup()|>
      add_row(data_name = factor('data/wiki', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
      add_row(data_name = factor('data/urls-short', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
      add_row(data_name = factor('rng4', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
      add_row(data_name = factor('int', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('baseline', levels = CONFIG_NAMES))|>
      filter(data_name %in% data_filter)


    colors <- c(
      brewer_pal(palette = 'RdBu')(8)[8],
      brewer_pal(palette = 'PuOr')(8)[2],
      brewer_pal(palette = 'RdBu')(8)[2],
      brewer_pal(palette = 'PRGn')(8)[2]
    );
    data|>
      #filter(reference == 'rhh',metric=='txs',config_name %in% c('hash','dense3'))|>
      ggplot(aes(x = ycsb_zipf, y = value, col = config_name)) +
      theme_bw() +
      facet_nested(. ~ data_name, labeller = labeller(
        op = OP_LABELS,
        data_name = DATA_LABELS,
      )) +
      scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 3) * ifelse(left, 0.5, 1), breaks = c(0:10) * ifelse(left, 0.5, 1)) +
      #geom_smooth(aes(x = ycsb_zipf, y = value, col = config_name),size=0.3) +
      geom_hline(yintercept = 1, col = brewer_pal(palette = 'RdBu')(8)[8]) +
      geom_point(aes(x = ycsb_zipf, y = value, col = config_name, fill = config_name), size = 0.1) +
      geom_text(data = labels, mapping = aes(x = x, y = y, col = config_name, hjust = h, vjust = v, label = c), size = 3) +
      scale_color_manual(values = colors, breaks = factor(c('baseline', 'art', 'hot', 'tlx'), levels = CONFIG_NAMES)) +
      #scale_fill_manual(values = colors) +
      scale_x_continuous(
        breaks = c(0.75,1,1.25),
        labels = label_percent(suffix = ''),
        name = 'Zipf-Parameter',
        expand = expansion(0),
      ) +
      expand_limits(y = c(0, 1.1)) +
      guides(fill = 'none', col = 'none') +
      labs(y = if(left){ 'Normalized op/s'}else{ NULL}) +
      theme(
        plot.margin = margin(0, 0, 0, 0),
        axis.text.x = element_text(size = 8,angle=45,hjust=1), ,
        #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
        axis.text.y = element_text(size = 8),
        axis.title.y = element_text(size = 8, hjust = 1),
        axis.title.x = element_text(size = 8),
        panel.spacing.x = unit(0, "mm"),
        #axis.ticks.x = element_blank(),
        strip.text = element_text(size = 8, margin = margin(2, 0, 2, 0)),
      )
  }

  hplot(TRUE) | hplot(FALSE) & theme(
    plot.margin = margin(0, 2, 0, 2),
  )
}
save_as('zipf-in-mem', 30)
