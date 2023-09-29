source('../common.R')

# ./page-size-run.py > errors
r <- rbind(
  read.csv('sparse-cpl.csv', strip.white = TRUE)
)

d <- r |>
  augment()|>
  filter(!is.nan(scale))|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  summarise(
    .by=c(config_name, op, psl, payload_size,data_name,keys_per_leaf),
    tp=mean(scale/time),n=n()
  )

d|>
  group_by(config_name, op, data_name,payload_size)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  ggplot()+
  facet_nested(data_name + op ~ config_name, scales = 'free_y') +
  geom_raster(aes(x=payload_size,y=psl,fill=tp/best_tp))+
  geom_line(aes(x=payload_size,y= log2(100*(2^psl / keys_per_leaf)) ))+
  scale_fill_viridis_c(limits = c(0.8,1),oob = scales::squish)+
  scale_y_continuous(labels = label_page_size)

d|>
  filter(payload_size==8 & config_name=='hints')|>
  group_by(config_name, op, data_name,payload_size)|>
  mutate(best_tp = max(tp,na.rm = TRUE))|>
  ggplot()+
  facet_nested(data_name + op ~ config_name, scales = 'free_y') +
  geom_raster(aes(x=payload_size,y=psl,fill=tp/best_tp))+
  scale_fill_viridis_c(limits = c(0.8,1),oob = scales::squish)+
  scale_y_continuous(labels = label_page_size)
