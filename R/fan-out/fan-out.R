source('../common.R')

r <- read_broken_csv('fan-out.csv.gz')

d <- r|>
  filter(op=='ycsb_c')|>
  transmute(tree_height, config_name, innerCount = nodeCount_Inner,
            data_name = factor(data_name, levels = names(DATA_MAP), labels = DATA_MAP),
            leafCount = nodeCount_Leaf +
              nodeCount_Hash +
              nodeCount_Dense +
              nodeCount_Dense2,
            avg_fan_out = (leafCount + innerCount -1) / innerCount,
            config_name = factor(config_name, levels = CONFIG_NAMES),)

d|>ggplot(aes(x=config_name,y=avg_fan_out,fill=config_name,label=leafCount))+
  geom_col()+
  geom_text(vjust=0)+
  facet_nested(.~data_name)

