source('../common.R')

r1<-read_broken_csv('../../random-1.csv.gz')
rhot<-read_broken_csv('../../random-hot.csv.gz')
rsparse<-read_broken_csv('../../random-sparse.csv.gz')
radapt<-read_broken_csv('../../random-adapt.csv.gz')

r <- bind_rows(
  r1,
  read_broken_csv('../../random-2.csv'),
  rhot,
  rsparse,
  radapt
)

d <- r |>
  augment()|>
  filter(op %in% c("ycsb_c", "ycsb_c_init", "ycsb_e"))|>
  filter(psi == 12 & psl == 12)|>
  filter(scale > 0)


d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  sample_n(10000,)|>
  ggplot()+
  facet_nested(data_name~config_name)+
  geom_point(aes(x=total_size,y=scale/time,col=payload_size))+
 # geom_smooth(aes(x=data_size,y=leaf_count/data_size,col=payload_size),method='lm')+
  scale_color_viridis_c()

d|>
  group_by(data_name)|>
  filter(config_name=='hash' | config_name=='hints')|>
  filter(final_key_count>0)|>
  mutate(x=final_key_count/leaf_count)|>
  summarise(x=mean(x))

#hash vs hint scatter
d|>
  filter(psl==12)|>
  filter(config_name=='hash'|config_name=='hints')|>
  filter(data_name %in% c('ints','sparse'))|>
  filter(op=='ycsb_c')|>
  mutate(
    entry_size = ifelse(config_name=='hash',(leaf_count*4096*0.75/final_key_count) - 7 ,(leaf_count*4096*0.75/final_key_count) - 10 )
  )|>
  ggplot()+
  facet_wrap(~data_name+cut(total_size,breaks=5)+cut(payload_size,5),scales='free_y')+
  geom_point( aes(y=scale/time,x=entry_size,col=config_name),alpha=0.5)

# config bars
d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(density>0.9 | data_name!='ints' |  config_name=='art')|>
  #filter(data_name!='ints' & op!='ycsb_e')|>
  filter(near(total_size, 3e8, 1e7))|>
  ggplot()+
  facet_nested(op~data_name,scales = 'free')+
  geom_bar(aes(x=config_name,y=scale/time,fill=config_name),stat='summary',fun=mean)+
  scale_fill_hue()+
  ylab('throughput')+
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))

# adapt config bars
d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(payload_size==8)|>
  filter(config_name %in% c('hints','hash','dense1','adapt','art','hot'))|>
  filter(density>0.9 | data_name!='ints' |  config_name=='art')|>
  #filter(data_name!='ints' & op!='ycsb_e')|>
  filter(near(total_size, 3e8, 1e7))|>
  ggplot()+
  facet_nested(op~data_name,scales = 'free')+
  geom_bar(aes(x=config_name,y=scale/time,fill=config_name),stat='summary',fun=mean)+
  scale_fill_hue()+
  ylab('throughput')+
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))

d|>
  filter(op=='ycsb_c')|>
  filter(payload_size==8)|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(config_name %in% c('hints','hash','dense1','adapt','art','hot'))|>
  filter(density>0.9 | data_name!='ints' |  config_name=='art')|>
  #filter(data_name!='ints' & op!='ycsb_e')|>
  filter(near(total_size, 3e8, 1e7))|>
  ggplot()+
  facet_nested(config_name~data_name)+
  geom_point(aes(x=payload_size,y=total_size))

#sparse config bars
d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(density>0.9 | data_name!='ints' |  config_name=='art')|>
  #filter(data_name!='ints' & op!='ycsb_e')|>
  filter(near(total_size, 3e8, 1e7))|>
  filter(data_name=='sparse')|>
  ggplot()+
  facet_wrap(~op,scales='free')+
  geom_bar(aes(x=config_name,y=scale/time,fill=config_name),stat='summary',fun=mean)+
  scale_fill_hue()+
  ylab('throughput')+
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))

#int art
d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(data_name=='ints')|>
  filter(near(total_size, 3e8, 1e7))|>
  filter(config_name %in% c('hints','dense1','dense2','art'))|>
  relocate(density)|>
  #sample_n(1e4)|>
  ggplot()+
  facet_nested(op~config_name,scales = 'free')+
  geom_point(aes(x=payload_size,y=scale/time,col=density))+
  scale_fill_hue()+
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))+
  scale_color_viridis_c(guide=guide_colorbar(title='integer density'))+
  ylab('throughout')+
  xlab('payload size')

#int density dense leaves
d|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(data_name=='ints')|>
  filter(near(total_size, 3e8, 1e7))|>
  filter(config_name %in% c('hints','dense1','dense2'))|>
  #sample_n(1e4)|>
  ggplot()+
  facet_nested(cut(density,breaks=5)~op,scales = 'free')+
  geom_point(aes(x=payload_size,y=scale/time,col=config_name))+
  geom_smooth(aes(x=payload_size,y=scale/time,col=config_name))+
  scale_color_hue()+
  scale_y_continuous(labels = label_number(scale_cut = cut_si("tx/s")))+
  ylab('throughout')+
  xlab('payload size')

d|>
  filter(data_name == 'urls')|>
  filter(psl==12 & psi==12 & payload_size<=16)|>
  filter(data_size>2.5e6)|>
  filter(config_name=='baseline')|>View()


if(FALSE){ # broken akima stuff


{#akima attempt 2
  library(akima)
  library(reshape2)
  df = d|>filter(op=='ycsb_c',data_name=='wiki',config_name=='hints')

  fld <- with(df, interp(x = payload_size, y = total_size/1e6, z = scale/time,duplicate='mean'))

  df <- melt(fld$z, na.rm = TRUE)
  names(df) <- c("x", "y", "z")
  df$payload_size <- fld$x[df$x]
  df$total_size <- fld$y[df$y] *1e6
  df|>View()

  ggplot(data = df, aes(x = payload_size, y = total_size, z = z)) +
    geom_tile(aes(fill = z)) +
    stat_contour()
}


  akima2dgroup <- function(d,xo,yo) {
    d|>group_modify(function(group,key){
      xmin<- min(xo)
      ymin<-min(yo)
      xrange <- max(xo)-xmin
      yrange <- max(yo)-ymin
      tryCatch(
      {
        fld<-with(group, interp(x = (x-xmin)/xrange, y = (y-ymin)/yrange, z = z,duplicate='mean',xo=(xo-xmin)/xrange,yo=(yo-ymin)/yrange))
        df <- melt(fld$z, na.rm = TRUE)
        names(df) <- c("xi", "yi", "z")
        df$x <- fld$x[df$xi] * xrange + xmin
        df$y <- fld$y[df$yi] * yrange + ymin
        df
      }
      )
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
    akima2dgroup(xo=seq(1e6,1e7,1e6),1:256)

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
