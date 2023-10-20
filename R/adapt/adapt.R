source('../common.R')

#r<-read_broken_csv('adapt-cmp-p16.csv')
#r <- read_broken_csv('../../adapt-cmp-seq.csv')
r <- bind_rows(
  read_broken_csv('adapt-cmp-seq.csv')|>filter(config_name!='hot'),
  read_broken_csv('adapt-cmp-seq-hot-fixed.csv.gz')
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(scale > 0)

d|>
  #filter(!(config_name %in% c('art','hot')))|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free') +
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_fill_hue() +
  ylab('throughput') +
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))

# title plot
d|>
  filter(config_name %in% c('baseline', 'hot', 'adapt'))|>
  filter(op == 'ycsb_c' | op == 'ycsb_e')|>
  mutate(
    config_name = fct_recode(config_name,
                             'baseline B-Tree' = 'baseline',
                             'HOT' = 'hot',
                             'adaptive B-Tree' = 'adapt'
    ),
    op = fct_recode(op,
                    'YCSB-C' = 'ycsb_c',
                    'YCSB-E' = 'ycsb_e',
    ),
    data_name = fct_recode(data_name,
                           'urls' = 'urls',
                           'Wikipedia titles' = 'wiki',
                           'dense integers' = 'ints',
                           'sparse integers' = 'sparse'
    ),
  )|>
  ggplot() +
  facet_nested(op ~ data_name, scales = 'free_y') +
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_fill_hue() +
  ylab(NULL) +
  xlab(NULL) +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank()) +
  scale_fill_brewer(palette = 'Dark2') +
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")), expand = expansion(add = c(0, 0.5e6))) +
  theme(legend.position = "bottom", legend.title = element_blank())


