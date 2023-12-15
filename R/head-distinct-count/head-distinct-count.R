source('../common.R')

COL_NAMES<-c('inner','distinct','records')
r<-bind_rows(
  read_csv('wiki.csv.gz',col_names = COL_NAMES)|>mutate(data='wiki'),
  read_csv('urls-short.csv.gz',col_names = COL_NAMES)|>mutate(data='urls'),
)

r|>ggplot()+
  facet_nested(inner~data,scales='free_y')+
  geom_density(aes(distinct/records))
