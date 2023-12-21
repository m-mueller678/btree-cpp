source("../common.R")


# r <- read_broken_csv('vary1.csv.gz')
r <- bind_rows(
  # python3 R/size3/leaf-size-density.py |parallel -j8 --joblog joblog -- {1}| tee R/size3/leaf-size-density.csv
  read_broken_csv('leaf-size-density.csv.gz')|>mutate(file=3),
)

d<-r|>augment()

d|>ggplot()+
  geom_line(aes(density,nodeCount_Dense/leaf_count,col=factor(psl)),stat='summary',fun=mean)+
  scale_color_hue()

d|>ggplot()+
  geom_line(aes(density,leaf_count*2^psl,col=factor(psl)),stat='summary',fun=mean)+
  scale_color_hue()