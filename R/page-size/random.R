source('../common.R')

# ./page-size-run.py > errors
r <- rbind(
  read_broken_csv('../../page-size-random-2.csv')
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(psi == 12)|>
  filter(scale > 0)

if (anyDuplicated(d['run_id', 'op'])) {
  stop('duplicated')
}

d|>
  filter(psi==12 & psl == 14 & config_name=='hints' & op=='ycsb_c' & data_name=='ints')|>
  mutate(leaf_space_per_key = leaf_count * 2^psl / final_key_count)|>
  ggplot() +
  facet_nested(psl ~ config_name) +
  geom_point(aes(x = payload_size, y = leaf_space_per_key, col = density,shape=data_name))+
  scale_color_viridis_c()

d|>
  filter(data_name!='ints' | density>0.9)|>
  ggplot() +
  facet_nested(op ~ config_name) +
  geom_point(aes(x =  final_key_count/leaf_count, y = scale/time, col = data_name,shape=factor(psl)))

d|>
  filter(data_name=='ints' & config_name=='dense1')|>
  ggplot()+
  geom_histogram(aes(density))


d|>
  count(config_name, psl)|>
  ggplot() +
  geom_raster(aes(x = config_name, y = psl, fill = n))+
  scale_fill_viridis_c()








