library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)


r = read.csv('../zipf_samples.csv',header=FALSE)
colnames(r) <- c('z')
data = head(r, n = 1000000)

ggplot(data, aes(z)) +
  stat_ecdf(data=data) +
  coord_cartesian(xlim=c(0, 10000))