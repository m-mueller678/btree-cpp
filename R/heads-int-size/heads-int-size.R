# investigate why heads have less pspace overhead on dense integers than on other keys
source('../common.R')

MAX_CAPACITY <- (4096 - 26 - 8) / (10 + 8 + c(4, 3, 2, 1))

KEY_COUNT_0<-25000000

d <- data_frame()

for (config in c('heads', 'prefix')) {
  for (data in c('dense', 'sparse')) {
    for (size in c('', '-2')) {
      key_count <- if (size == '') { KEY_COUNT_0 } else { 20000000 }
      d <- bind_rows(d,
                     read_csv(paste0(data, '-', config, size, '.csv.gz'), col_names  = c('prefix', 'count'))|>
                       mutate(data = data, config = config, key_count = key_count)
      )
    }
  }
}

counted <- d|>
  count(data, config,key_count, prefix, drop = FALSE)

leaf_count <- counted|>
  group_by(data, config,key_count)|>
  summarise(n = sum(n),.groups = 'drop')

#25: sparse has less leaves than dense on prefix

leaf_count|>
  pivot_wider(names_from = c('config'), values_from = 'n')|>
  mutate(
    r = heads / prefix,
    d = (heads - prefix) * 4096 / KEY_COUNT,
  )

counted|>
  ggplot() +
  facet_wrap(~data + config) +
  geom_col(aes(x = prefix, y = n))

d|>
  ggplot() +
  facet_nested(key_count~ .) +
  geom_density(aes(x = count,y = ..density.., col = interaction(config,data)),alpha=0.5)

d|>
  ggplot() +
  facet_nested(key_count~ .) +
  geom_density(aes(x = 4096/count,y = ..density.., col = interaction(config,data)),alpha=0.5,bw=5)


d|>
  ggplot() +
  facet_nested(key_count~ .) +
  geom_histogram(aes(x = 4096/count,y = ..density.., fill = interaction(config,data)),position = 'dodge',binwidth = 5)

d|>
  ggplot() +
  facet_nested(key_count~ .) +
  geom_histogram(aes(x = count,y = ..density.., fill = interaction(config,data)),position = 'dodge')