source('../common.R')

#parallel -j1 --joblog joblog -- DATA={2} KEY_COUNT={1} YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=1 OP_COUNT=1e5 PAYLOAD_SIZE={3} ZIPF=0.99 DENSITY=1 {4} ::: $(seq 5000000 2500000 25000000) ::: int rng4 ::: $(seq 0 16) ::: named-build/prefix-n3-ycsb named-build/heads-n3-ycsb | tee R/heads-int-size/pl.csv
#r<-read_broken_csv('pl.csv')
# with prefix length
r<-read_broken_csv('pl-2.csv')

d<-augment(r)|>
  mutate(space_per_key=leaf_count*4096/final_key_count)

OUTPUT_COLS<-c(OUTPUT_COLS,'space_per_key')

config_pivot <- d|>
  pivot_wider(id_cols = (!any_of(c(OUTPUT_COLS, 'bin_name', 'run_id'))), names_from = config_name, values_from = any_of(OUTPUT_COLS), values_fn = mean)

config_pivot$avg_leaf_prefix_prefix


config_pivot|>
  ggplot()+
  facet_nested(data_name~.)+
  geom_point(aes(y=space_per_key_heads - space_per_key_prefix,x=payload_size,col=data_size))+
  scale_color_viridis_c()

# jump at pl=8 for prefix, but pl=4 for heads
d|>
  ggplot()+
  facet_nested(data_name~.)+
  geom_point(aes(y=space_per_key,x=payload_size,col=config_name))+
  #geom_smooth(aes(y=space_per_key,x=payload_size,col=config_name),method = 'lm')+
  xlim(c(8,32))

d|>
  filter(data_name=='ints')|>
  ggplot()+
  facet_nested(data_name~.)+
  geom_boxplot(aes(y=space_per_key,x=payload_size,col=config_name,group=interaction(payload_size,config_name)),position='identity',linewidth=0.2)+
  #geom_point(aes(y=space_per_key,x=payload_size,col=config_name))+
  geom_smooth(aes(y=space_per_key,x=payload_size,col=config_name),linewidth=0.5,method = 'lm')+
  coord_cartesian(xlim=c(0,20),ylim=c(10,40))+
  scale_x_continuous()

d|>
  ggplot()+
  facet_nested(data_name~config_name)+
  geom_point(aes(y=space_per_key,x=payload_size))

# jump around 175 keys per leaf
d|>
  ggplot()+
  facet_nested(data_name~config_name)+
  geom_point(aes(y=keys_per_leaf,x=payload_size))+
  coord_cartesian(ylim = c(150,250),xlim=c(0,15))

((
   d|>
     filter(data_name=='ints')|>
     ggplot()+theme_bw()+
     geom_point(aes(y=space_per_key,x=payload_size,col=config_name))+
     scale_x_continuous(expand = expansion(0),breaks=(0:50)*5)+
     coord_cartesian(ylim = c(10,34),xlim=c(0,16))+
     scale_y_continuous(expand = expansion(0))+
     xlab('value size')+
     ylab('space per record')+
     scale_color_brewer(type='qual',palette = 'Dark2',labels = CONFIG_LABELS)+
     guides(col = FALSE)
 )|
(d|>
  filter(data_name=='ints')|>
  ggplot()+theme_bw()+
  #geom_point(aes(y=keys_per_leaf,x=payload_size,col=config_name))+
  geom_boxplot(aes(y=space_per_key,x=factor(payload_size),col=config_name,group=interaction(config_name,payload_size)),position = 'identity')+
  coord_cartesian(ylim = c(20,26),xlim=c(2,10))+
  scale_x_discrete(breaks=(1:50)*2,expand=expansion(0))+
  xlab('value size')+
  scale_y_continuous(expand = expansion(0),breaks=(1:50)*2)+
  theme(axis.title.y = element_blank())+
  scale_color_brewer(type='qual',palette = 'Dark2',labels = CONFIG_LABELS)+
  guides(col = FALSE)
)|
(d|>
  filter(data_name=='ints')|>
  ggplot()+
  #facet_wrap(~config_name)+
  theme_bw()+
  geom_point(aes(y=space_per_key,x=avg_leaf_prefix,col=config_name))+
  coord_cartesian(ylim = c(20,26),xlim=c(2.1,2.5))+
  scale_x_continuous(expand = expansion(0),breaks=(11:12)*0.2)+
  xlab('prefix length')+
  scale_y_continuous(expand = expansion(0),breaks=(1:50)*2)+
  theme(axis.title.y = element_blank())+
  scale_color_brewer(type='qual',palette = 'Dark2',labels = CONFIG_LABELS,name="Configuration")+
  guides(color = guide_legend(
    override.aes = list(size = 5))
  )
))+plot_layout(guides = "collect")&theme(legend.position = 'top',text = element_text(size = 24))

d|>
  filter(data_name=='ints')|>
  ggplot()+
  facet_wrap(~config_name)+
  geom_point(aes(y=space_per_key,x=avg_leaf_prefix))+
  coord_cartesian(ylim = c(20,26),xlim=c(2,2.5))+
  scale_x_continuous(expand = expansion(0))+
  scale_y_continuous(expand = expansion(0))

d|>
  ggplot()+
  facet_nested(data_name~config_name)+
  geom_tile(aes(y=final_key_count,x=payload_size,fill=leaf_count))+
  scale_fill_viridis_c()

config_pivot|>
  group_by(data_name)|>
  summarise(d=mean(space_per_key_heads-space_per_key_prefix))