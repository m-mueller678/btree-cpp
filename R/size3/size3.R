source("../common.R")

# python3 R/size3/vary1.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary1.csv
# r <- read_broken_csv('vary1.csv.gz')
r <- bind_rows(
  # python3 R/size3/vary2.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary2.csv
  read_broken_csv('vary2.csv.gz'),
  # python3 R/size3/vary3.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary3.csv
  read_broken_csv('vary3.csv.gz'),
)

d <- r|>
  filter(op!='ycsb_e_init')|>
  augment()|>
  filter(pmin(psl,psi)>=case_when(data_name == 'urls' ~ 11,data_name=='wiki'~10,TRUE~8))

v2 <- bind_rows(
  d|>mutate(inner = TRUE),
  d|>mutate(inner = FALSE),
)|>
  filter((inner | psi == 12))|>
  filter(!inner | config_name=='hints')|>
  filter(!inner | psl == 12)|>
  mutate(psv = ifelse(inner, psi, psl))

v1 <- v2|>filter(payload_size==8)

v1|>
  ggplot() +
  theme_bw() +
  facet_nested(inner+op~data_name,scales='free_y',independent = 'y')+
  geom_line(aes(psv, txs, col = config_name),stat='summary',fun=length)+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))

v2|>
  filter(config_name %in% c('prefix','heads','hints','hash') | config_name %in% c('dense2','dense3') & data_name=='ints')|>
  #filter(config_name=='hash')|>
  filter(!inner)|>
  ggplot() +
  theme_bw() +
  facet_nested(op+payload_size~config_name,scales='free_y',independent = 'y')+
  #facet_wrap(~data_name+inner+op,scales='free_y')+
  geom_line(aes(psv, txs, col = data_name),stat='summary',fun=length)+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))

v2|>
  filter(!inner | config_name=='hints')|>
  mutate(group=factor(case_when(
    inner~'inner',
    grepl('dense',config_name)~'dense',
    config_name %in% c('hash','baseline','prefix') ~ 'no-head',
    TRUE ~ 'head'
  ),levels=c('no-head','head','dense','inner')))|>
  #filter(config_name %in% c('hints','hash','dense1','dense3'))|>
  #filter(!inner)|>
  ggplot() +
  theme_bw() +
  facet_nested(
    op+payload_size~group+config_name,
               scales='free_y',
               independent = 'y',
  )+
  geom_line(aes(psv, txs, col = data_name),stat='summary',fun=mean)+
  #geom_smooth(aes(psv, txs, col = data_name),method = 'lm')+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))+
  expand_limits(y=0)+
  geom_vline(xintercept = 12)

v1|>
  filter(!inner | config_name=='hints')|>
  mutate(group=factor(case_when(
    inner~'inner',
    grepl('dense',config_name)~'dense',
    config_name %in% c('hash','baseline','prefix') ~ 'no-head',
    TRUE ~ 'head'
  ),levels=c('no-head','head','dense','inner')))|>
  #filter(config_name %in% c('hints','hash','dense1','dense3'))|>
  #filter(!inner)|>
  filter(op=='ycsb_e')|>
  pivot_longer(any_of(OUTPUT_COLS),names_to = 'metric')|>
  filter(metric %in% c('cycle','L1_miss','LLC_miss','instr','br_miss'))|>
  ggplot() +
  theme_bw() +
  facet_nested(
    metric~group+config_name,
    scales='free_y',
    independent = 'y',
  )+
  geom_line(aes(psv, 1/value, col = data_name),stat='summary',fun=mean)+
  #geom_smooth(aes(psv, txs, col = data_name),method = 'lm')+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))+
  expand_limits(y=0)

v1|>filter(inner)|>select(config_name)|>unique()

v1|>
  filter(inner)|>
  group_by(config_name,op,data_name)|>
  filter(psv>=11)|>
  arrange(psv,.by_group = TRUE)|>
  summarize(
    minmax = first(txs)/last(txs),
    worstbest = min(txs)/max(txs),
  )|>
  arrange(worstbest)

v1|>
  group_by(data_name,inner,config_name)|>
  summarize(min_size=min(psv))|>View()