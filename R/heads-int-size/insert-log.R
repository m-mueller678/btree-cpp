source('../common.R')


# parallel --joblog joblog -j4 -- env DATA=int KEY_COUNT=40e6 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE={2} ZIPF=0.99 DENSITY=1 named-build/{1}-n3-ycsb \| gzip \> R/heads-int-size/insert-log/{1}-pl{2}-r{3}.csv.gz ::: prefix heads ::: 6 7 8 9 10 11 12 ::: 0 1 2 3 4
# parallel --joblog joblog -j4 -- env DATA=rng4 KEY_COUNT=4e7 YCSB_VARIANT=3 SCAN_LENGTH=100 RUN_ID=0 OP_COUNT=1e7 PAYLOAD_SIZE={2} ZIPF=0.99 DENSITY=1 named-build/{1}-n3-ycsb \| gzip \> R/heads-int-size/insert-log/sparse-{1}-pl{2}-r{3}.csv.gz ::: prefix heads ::: 6 8 10 ::: 0 1 2

files <- fs::dir_ls(path='insert-log/',glob = '*.csv.gz')
files<-files[grepl('r0',files)]
files<-files[grepl('pl(6|8)',files)]
r<-vroom::vroom(files, id = "file",delim=',',col_names = 'leaves')|>
  group_by(file)|>
  mutate(count=row_number())|>
  filter(count%%100==0)

d<- r|>extract(file,c('data','config','pl','run_id'),'insert-log/([[:alnum:]]+)-([[:alnum:]]+)-pl([0-9]+)-r([0-9]+).csv.gz')|>
  mutate(pl=factor(as.integer(pl)))

d|>
  ggplot()+
  facet_nested(config+pl~data)+
  geom_line(aes(count,count/leaves,col=run_id))+
  scale_color_discrete()

d|>filter(pl==8)|>
  ggplot()+
  facet_nested(run_id~data)+
  geom_line(aes(count,count/leaves,col=config))+
  scale_color_discrete()

d|>
  pivot_wider(names_from = config,values_from = 'leaves')|>
  ggplot()+
  facet_nested(run_id~pl)+
  geom_line(aes(count,heads/prefix,col=data))+
  scale_x_log10()+
  scale_color_discrete()+
  geom_vline(xintercept = 25e6)+
  coord_cartesian(ylim=c(1.2,1.35),xlim=c(1e5,1e8))

d|>
  pivot_wider(names_from = config,values_from = 'leaves')|>
  ggplot()+
  facet_nested(run_id~pl)+
  geom_line(aes(count,(heads-prefix)*4096/count,col=data))+
  scale_x_log10()+
  scale_color_discrete()+
  geom_vline(xintercept = 25e6)+
  coord_cartesian(xlim=c(1e5,1e8))


d|>
  filter(pl==8,data=='int')|>
  ggplot()+
  geom_line(aes(count,leaves,col=config))+
  geom_vline(xintercept = 25e6)

d|>
  filter(pl==8,data=='int')|>
  filter(count==25e6)



