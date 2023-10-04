source('../common.R')

r <- rbind(
  read_broken_csv('../../page-size-random-2.csv')|>mutate(rand_seed = NaN),
  read_broken_csv('../../page-size-random-3.csv'),
  read_broken_csv('../../page-size-random-4.csv')
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(psi == 12 & psl == 12)|>
  filter(scale > 0)

d|>
  filter(data_size>1e6)|>
  group_by(config_name, op, data_name)|>
  mutate(
  tp = scale / time,
  tpl=log10(tp),
  z = (tpl-min(tpl)) / (max(tpl)-min(tpl))
)|>
  ggplot()+
  facet_nested(config_name~ op+ data_name)+
  geom_point(aes(x=total_size,y=payload_size,col=z))+
  scale_color_viridis_c()



{ # broken akima stuff
  akima2d <- function(d,xo,yo) {
    d|>group_modify(function(group,key){
      xscale <- max(xo) - min(xo)
      yscale <- max(yo) - min(yo)
      if(nrow(group)<4 | xscale==0 | yscale==0){
        return(data.frame(x=NULL, y= NULL, z=NULL))
      }
      akima <- akima::interp(x = group$x / xscale, y = group$y / yscale, z = group$z, duplicate = 'mean',xo=xo/xscale,yo=yo/yscale)
      mat<-akima$z
      mapped <- data.frame(
        xi = rep(1:nrow(mat), each = ncol(mat)),
        yi = rep(1:ncol(mat), times = nrow(mat)),
        z = as.vector(mat)
      )
      mapped|>
        mutate(x = xo[xi], y = yo[yi])|>
        select(x, y, z)
    })
  }

  out<- d|>
    group_by(config_name, op, data_name)|>
    mutate(
      tp = scale / time,
      max_tp = max(tp),
      x = total_size,
      y = payload_size,
      z = tp / max_tp,
      .keep = 'none'
    )|>
    akima2d()

  d|>
    group_by(config_name, op, data_name)|>
    mutate(
      tp = scale / time,
      max_tp = max(tp),
      x = total_size,
      y = payload_size,
      z = total_size,
      .keep = 'none'
    )|>
    akima2d(xo=seq(1e6,1e7,1e6),yo=seq(1,256,8))|>
    # filter(op=='ycsb_c' & config_name == 'hints' & data_name=='ints')|>View()
    ggplot() +
    facet_nested(config_name ~ op + data_name) +
    geom_tile(aes(y = y, x = x, fill = z)) +
    scale_color_viridis_c()

}
