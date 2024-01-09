r<- read_broken_csv('alignment.csv')

d<- r|>augment()

pivoted<-d |>
  pivot_longer(any_of(OUTPUT_COLS),names_to = 'metric')|>
  mutate(overaligned = case_when(bin_name == './hints-over-align'~'page',bin_name=='./hints'~'min',TRUE~NA),
         config_name,data_name,data_size,op,payload_size,metric,value,
         .keep = 'none')|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS,'bin_name', 'run_id'))), names_from = overaligned, values_from = 'value', values_fn = mean)

pivoted|>
  filter(metric %in% c('txs',"L1_miss","LLC_miss"))|>
  ggplot()+
  facet_nested(metric~payload_size+data_name)+
  geom_bar(aes(x=op,y=page/min-1,fill=op),stat='summary',fun=mean)+
  scale_y_continuous(labels = label_percent())+
  scale_fill_brewer(palette = 'Dark2')

