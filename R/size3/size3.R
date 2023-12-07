source("../common.R")

# python3 R/size3/vary1.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary1.csv
r <- read_broken_csv('vary1.csv.gz')
# python3 R/size3/vary2.py |parallel -j1 --joblog joblog -- {1}| tee R/size3/vary2.csv
r <- read_broken_csv('vary2.csv.gz')

d <- r|>
  filter(op!='ycsb_e_init')|>
  augment()|>
  filter(pmin(psl,psi)>=case_when(data_name == 'urls' ~ 11,data_name=='wiki'~10,TRUE~8))

v1 <- bind_rows(
  d|>mutate(inner = TRUE),
  d|>mutate(inner = FALSE),
)|>
  filter(inner | psi == 12)|>
  filter(!inner | psl == 12)|>
  mutate(psv = ifelse(inner, psi, psl))

v1|>
  ggplot() +
  theme_bw() +
  facet_nested(inner+op~data_name,scales='free_y',independent = 'y')+
  geom_line(aes(psv, txs, col = config_name),stat='summary',fun=mean)+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))

v1|>
  #filter(config_name %in% c('hints','hash','dense1','dense3'))|>
  #filter(!inner)|>
  ggplot() +
  theme_bw() +
  facet_nested(inner+op~config_name,scales='free_y',independent = 'y')+
  geom_line(aes(psv, txs, col = data_name),stat='summary',fun=mean)+
  geom_smooth(aes(psv, txs, col = data_name),method = 'lm')+
  scale_color_brewer(palette = 'Dark2')+
  scale_y_continuous(name = NULL, labels = label_number(scale_cut = cut_si('op/s')))+
  expand_limits(y=0)


v1|>
  group_by(data_name,inner,config_name)|>
  summarize(min_size=min(psv))|>View()