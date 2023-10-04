source('../common.R')

# ./page-size-run.py > errors
r <- rbind(
  read_broken_csv('../../page-size-random-2.csv')|>mutate(rand_seed = NaN),
  read_broken_csv('../../page-size-random-3.csv')
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(psi == 12)|>
  filter(scale > 0)

if (anyDuplicated(d['run_id', 'op'])) {
  stop('duplicated')
}

# space efficiency
d|>
  #filter(psi==12 & psl == 14 & config_name=='hints' & op=='ycsb_c' & data_name=='ints')|>
  mutate(leaf_space_per_key = leaf_count * 2^psl / final_key_count)|>
  ggplot() +
  facet_nested(psl ~ config_name) +
  geom_point(aes(x = payload_size, y = (payload_size + avg_key_size) * data_size / (leaf_count * 2^psl), col = data_name)) +
  ylim(c(0, 3))

# space per key
d|>
  #filter(psi==12 & psl == 14 & config_name=='hints' & op=='ycsb_c' & data_name=='ints')|>
  mutate(leaf_space_per_key = leaf_count * 2^psl / final_key_count)|>
  ggplot() +
  facet_nested(psl ~ config_name) +
  geom_point(aes(x = payload_size, y = leaf_space_per_key, col = data_name))

# payload size vs throughput
{
  make_plot <- function(select_int) {
    d|>filter(xor(select_int ,data_name == 'ints') )|>
      filter(data_name != 'ints' | density > 0.75)|>
      filter(scale/time<if_else(op=='ycsb_e',1e6,6e6))|>
      filter(near(total_size, 3e8, 1e3))|>
      filter(payload_size <= 256)|>
      mutate(keys_per_leaf = final_key_count / leaf_count)|>
      ggplot() +
      facet_nested(op ~ data_name + config_name, scales = 'free_y') +
      geom_point(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      #geom_smooth(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      scale_y_continuous(
        labels = labels
      ) +
      scale_x_continuous(
        labels = label_number(scale_cut = cut_si('B')),
        #limits = c(0,256)
      ) +
      scale_color_viridis_d(limits = ordered(8:15))
  }
  ((make_plot(TRUE) | make_plot(FALSE)))  + plot_layout(guides = "collect")  &
    theme(legend.position = 'bottom', legend.direction = 'horizontal')
}

d|>
  filter(data_name != 'ints' | density > 0.75)|>
  group_by(op,data_name,config_name,psl)|> # for each group predict throughput depending on payload size
  summarise(n=n())|>
  View()

{
  make_plot <- function(select_int) {
    d|>filter(xor(select_int ,data_name == 'ints') )|>
      filter(data_name != 'ints' | density > 0.75)|>
      filter(scale/time<if_else(op=='ycsb_e',1e6,6e6))|>
      filter(near(total_size, 3e8, 1e3))|>
      filter(payload_size <= 256)|>
      filter(final_key_count/leaf_count<500)|>
      mutate(keys_per_leaf = final_key_count / leaf_count)|>
      ggplot() +
      facet_nested(op ~ data_name + config_name, scales = 'free') +
      #geom_point(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      geom_smooth(aes(x = final_key_count/leaf_count, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      scale_y_continuous(
        labels = labels
      ) +
      scale_color_viridis_d(limits = ordered(8:15))
  }
  ((make_plot(TRUE) | make_plot(FALSE)))  + plot_layout(guides = "collect")  &
    theme(legend.position = 'bottom', legend.direction = 'horizontal')
}

d|>
  filter(data_name == 'ints' & config_name == 'dense1')|>
  ggplot() +
  geom_histogram(aes(density))


d|>
  count(config_name, psl)|>
  ggplot() +
  geom_raster(aes(x = config_name, y = psl, fill = n)) +
  scale_fill_viridis_c()


# payload size vs throughput, only best size

{
  make_plot <- function(select_int) {
    d|>group_by(op,data_name,config_name,payload_size)|>
      slice_max(order_by = scale/time)|># for each group predict throughput depending on payload size
      filter(xor(select_int ,data_name == 'ints') )|>
      filter(data_name != 'ints' | density > 0.75)|>
      filter(scale/time<if_else(op=='ycsb_e',1e6,6e6))|>
      filter(near(total_size, 3e8, 1e3))|>
      filter(payload_size <= 256)|>
      mutate(keys_per_leaf = final_key_count / leaf_count)|>
      ggplot() +
      facet_nested(op ~ data_name + config_name, scales = 'free_y') +
      geom_point(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      #geom_smooth(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15))) + # alt: keys per leaf
      scale_y_continuous(
        labels = labels
      ) +
      scale_x_continuous(
        labels = label_number(scale_cut = cut_si('B')),
        #limits = c(0,256)
      ) +
      scale_color_viridis_d(limits = ordered(8:15))
  }
  ((make_plot(TRUE) | make_plot(FALSE)))  + plot_layout(guides = "collect")  &
    theme(legend.position = 'bottom', legend.direction = 'horizontal')
}

d|>
     filter(data_name != 'ints' | density > 0.75)|>
    ggplot() +
    facet_nested(op ~ data_name + config_name, scales = 'free') +
    geom_point(aes(x = payload_size, y = scale / time, col = ordered(psl,levels = 8:15)))  # alt: keys per leaf

