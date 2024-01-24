source("../common.R")


# r <- read_broken_csv('vary1.csv.gz')
r <- bind_rows(
  # not enough runs
  # python3 R/size3/vary1.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary1.csv
  #read_broken_csv('vary1.csv.gz'),

  # broken, only varies inner size
  # python3 R/size3/vary2.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary2.csv
  #read_broken_csv('vary2.csv.gz'),

  # large payloads, incomplete
  # python3 R/size3/vary3.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary3.csv
  # read_broken_csv('vary3.csv.gz')|>mutate(file=3),

  # incomplete
  # python3 R/size3/vary4.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary4.csv
  #read_broken_csv('vary4.csv.gz'),

  # python3 R/size3/vary5.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary5.csv
  # read_broken_csv('vary5.csv.gz')|>mutate(file=5),

  # christmas run
  read_broken_csv('vary6.csv.gz')|>mutate(file=5),
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
  #filter(!inner | config_name=='hints')|>
  filter(!inner | psl == 12)|>
  mutate(psv = ifelse(inner, psi, psl))

v1 <- v2|>filter(file==5)

v2|>
  filter(config_name %in% c('prefix','heads','hints','hash') | config_name %in% c('dense2','dense3') & data_name=='ints')|>
  #filter(config_name=='hash')|>
  filter(!inner)|>
  ggplot() +
  theme_bw() +
  facet_nested(op+payload_size~config_name,scales='free_y',independent = 'y')+
  #facet_wrap(~data_name+inner+op,scales='free_y')+
  geom_line(aes(psv, txs, col = data_name),stat='summary',fun=mean)+
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
  filter(psv>=10)|>
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
  geom_line(aes(psv, value, col = data_name),stat='summary',fun=mean)+
  #geom_smooth(aes(psv, txs, col = data_name),method = 'lm')+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('')))+
  expand_limits(y=0)

v1|>filter(inner)|>select(config_name)|>unique()

v1|>
  filter(psv>=10)|>
  group_by(config_name,op,data_name,inner,psv)|>
  summarize(txs=mean(txs),.groups = 'drop_last')|>
  arrange(psv,.by_group = TRUE)|>
  summarize(
    minmax = first(txs)/last(txs),
    worstbest = min(txs)/max(txs),
  )|>
  arrange(worstbest)

# abs
v1|>filter(op=='scan',config_name=='hash',!inner)|>
  ggplot()+
  facet_wrap(~payload_size)+
  geom_line(aes(psv,L1_miss,col=data_name),stat='summary',fun=mean)+
  coord_cartesian(ylim=c(0,100))

# normalized txs / psv

v1|>
  filter(psv>=10,payload_size==8)|> #smaller than 1KiB is always worse
  group_by(config_name,op,data_name,inner,psv)|>
  summarize(mean_txs = mean(txs),.groups = 'drop_last')|>
  mutate(
    max_txs = mean_txs[psv==12],.groups='drop',
    normalized_txs = mean_txs/max_txs,
    variation = paste0(CONFIG_LABELS[config_name],ifelse(inner,' (inner)',''))
  )|>
  #pivot_wider(values_from = 'normalized_txs', names_from = 'config_name',id_cols = any_of(c('config_name','op','data_name','inner','psv')))|>
  filter(config_name %in% ifelse(inner,c('dense3'),c('hash','dense3')))|>
  ggplot()+theme_bw()+
  facet_nested(op~inner+config_name+variation,scales='free',
               labeller = labeller(
                 inner=NULL,
                 config_name = CONFIG_LABELS,
                 op=OP_LABELS,
               ),
               strip= strip_nested(
                 text_x = list(element_blank(),element_blank(), element_text()),
                 background_x = list(element_blank(),element_blank(), element_rect()),
                 by_layer_x = TRUE
               )
  )+
  geom_line(aes(psv,normalized_txs,col=data_name))+
  scale_y_continuous(labels = label_percent(),name='op/s relative to 4KiB',breaks = (0:100)*0.2)+
  scale_x_continuous(breaks = (0:100)*2,name="Node Size (KiB)",
                     labels = function(x) 2^(x-10))+
  scale_color_brewer(palette = 'Dark2',labels = DATA_LABELS)+
  theme(legend.position =  'bottom',
        strip.text=element_text(size=7.5),
        plot.margin = margin(-30,0,0,0),
        legend.title = element_blank(),
        legend.margin = margin(0,0,0,0),
        legend.box.margin = margin(0,0,0,0),
  )+
  coord_cartesian(ylim=c(0.6,1.3))
save_as('node-size-08',70)



# keys per leaf
v1|>
  filter(psv>=10,!inner)|> #smaller than 1KiB is always worse
  filter(config_name %in% ifelse(inner,c('dense3'),c('hash','dense3')))|>
  group_by(config_name,inner,op,psv,data_name)|>
  summarize(txs=mean(txs),keys_per_leaf=mean(keys_per_leaf))|>
  group_by(config_name,inner,op,data_name)|>
  mutate(normalized_tx=txs/max(txs))|>
  ggplot()+theme_bw()+
  facet_nested(op~inner+config_name,scales='free',
               labeller = labeller(
                 config_name = CONFIG_LABELS,
                 op=OP_LABELS,
               ),
  )+
  scale_y_continuous(labels = label_percent(),name=NULL,breaks = (0:100)*0.1)+
  geom_line(aes(keys_per_leaf,normalized_tx,col=data_name))+
  scale_color_brewer(palette = 'Dark2')+
  theme(legend.position =  'bottom',
        legend.title = element_blank(),
  )

