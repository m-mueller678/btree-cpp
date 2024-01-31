source('../common.R')

# parallel -j1 --joblog joblog -- env -S {3} YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e7 PAYLOAD_SIZE=8 ZIPF={1} DENSITY=1 {4} ::: $(seq -w 50 1 150 | xargs -I{} echo "scale=2;{}/100" | bc) ::: 3 :::  'DATA=data/urls-short KEY_COUNT=4273260' 'DATA=data/wiki KEY_COUNT=9818360' 'DATA=int KEY_COUNT=25000000' 'DATA=rng4 KEY_COUNT=25000000' ::: named-build/*-n3-ycsb | tee R/in-memory-skew/seq.csv
# r<-read_broken_csv('seq.csv')

# python3 R/in-memory-skew/skew.py |parallel -j1 --joblog joblog --retries 3 -- {1}| tee R/in-memory-skew/skew.csv
#r <- read_broken_csv('skew.csv')
# python3 R/in-memory-skew/skew.py |parallel -j1 --joblog joblog --retries 3 -- {1}| tee R/in-memory-skew/skew2.csv
# christmas run
r <- bind_rows(
  read_broken_csv('skew3b.csv.gz')
)

d <- r |>
  filter(op == 'ycsb_c')|>
  augment()|>
  mutate(
    round_size = cut(total_size, breaks = c(37e6, 38e6, 74e6, 76e6, 149e6, 151e6, 299e6, 301e6, 599e6, 601e6, 1199e6, 1201e6), labels = c(37.5, NA, 75, NA, 150, NA, 300, NA, 600, NA, 1200))
  )

d|>group_by(data_name,config_name)|>count()|>filter(n!=255)|>View()

cp <- {
  cp <- d|>
    pivot_longer(any_of(OUTPUT_COLS), names_to = 'metric')|>
    pivot_wider(id_cols = !any_of(c('bin_name', 'run_id', 'config_name')), names_from = 'config_name', values_from = 'value', values_fn = mean, names_prefix = 'x-')

  configs = unique(d$config_name)
  for (c in configs) {
    cb <- paste0('x-', c)
    cp[paste0('ra-', c)] <- ifelse(cr != 'x-NA', cp[cb] / cp['x-adapt2'], NA)
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


# in-mem
mem_zipf_plot<-function(values_prefix){
  data <- d|>
    pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = median)|>
    select(contains(values_prefix),op,data_name,ycsb_zipf)|>
    mutate(ra=!!sym(paste0(values_prefix,'adapt2')))|>
    pivot_longer(contains(values_prefix),names_prefix = values_prefix)|>
    mutate(config_name=factor(name,levels=CONFIG_NAMES),value=value/ra)|>
    #filter(data_name %in% c('urls', 'sparse', 'ints'))|>
    filter(config_name %in% c('art', 'hot','tlx','wh'))

  labels <- data|>
    group_by(data_name, config_name)|>
    filter(ycsb_zipf < 1.0)|>
    summarize(
      #up=config_name!='tlx',
      max = max(value),
      min = min(value),
    )|>
    mutate(
      above = !(config_name=='tlx' | data_name %in% c('ints','sparse') & config_name=='hot'),
      y = ifelse(above , max, min),
      x = ifelse(config_name=='tlax',1,0.5),
      h = ifelse(config_name=='tlax',0.5,0),
      v = ifelse(above , -0.1, 1.1),
      c = CONFIG_LABELS[config_name],
      config_name,
      .keep = 'none'
    )|>
    ungroup()|>
    add_row(data_name = factor('data/wiki', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
    add_row(data_name = factor('data/urls-short', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
    add_row(data_name = factor('rng4', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))|>
    add_row(data_name = factor('int', levels = names(DATA_MAP), labels = DATA_MAP), x = 1.5, y = 1, h = 1, v = -0.2, c = 'B-Tree', config_name = factor('adapt2', levels = CONFIG_NAMES))


  colors <- c(
    brewer_pal(palette = 'RdBu')(8)[8],
    brewer_pal(palette = 'PuOr')(8)[2],
    brewer_pal(palette = 'RdBu')(8)[2],
    brewer_pal(palette = 'PRGn')(8)[2],
    brewer_pal(palette = 'PiYG')(8)[7]
  );
  data|>
    ggplot() +
    theme_bw() +
    facet_nested(. ~ data_name, scales = 'free', labeller = labeller(
      op = OP_LABELS,
      data_name = DATA_LABELS,
    )) +
    # facetted_pos_scales(
    #   y = list(
    #     scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 2), breaks = c(0:10) * 0.5),
    #     scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 3), breaks = c(0:10)),
    #     scale_y_continuous(expand = expansion(mult = c(0, 0.05)), limits = c(0, 3), breaks = c(0:10))
    #   )
    # ) +
    scale_y_log10(expand = expansion(mult = c(0, 0)),breaks = c(1/4,1/2,1,2,4),labels = c("1/4","1/2",1,2,4))+
    coord_cartesian(ylim= c(.25, 4))+
    geom_hline(yintercept = 1, col = brewer_pal(palette = 'RdBu')(8)[8],linewidth=0.25,linetype='dashed') +
    geom_line(mapping=aes(x = ycsb_zipf, y = value, col = config_name),linewidth=0.5,alpha=0.9) +
    geom_text(data = labels, mapping = aes(x = x, y = y, col = config_name, hjust = h, vjust = v, label = c), size = 3) +
    scale_color_manual(values = colors, breaks = factor(c('adapt2', 'art', 'hot', 'tlx','wh'), levels = CONFIG_NAMES)) +
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
      axis.text.x = element_text(size = 8,hjust=c(0,0.5,1)) ,
      #axis.text.x = element_text(angle = 90,hjust=1,vjust=0.5),
      axis.text.y = element_text(size = 8),
      axis.title.y = element_text(size = 8, hjust = 0.6),
      axis.title.x = element_text(size = 8),
      panel.spacing.x = unit(1, "mm"),
      #axis.ticks.x = element_blank(),
      plot.margin = margin(0, 8, 0, 0),
    ) +
    labs(y = 'Normalized lookup/s (log)')
}
mem_zipf_plot('txs_')
save_as('zipf-in-mem', 35)

mem_zipf_plot('txs_')/mem_zipf_plot('instr_')/mem_zipf_plot('L1_miss_')

