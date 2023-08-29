library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)


r = read.csv('../zipf_samples.csv',header=FALSE)
colnames(r) <- c('z')
data = head(r, n = 1000)

ggplot(data, aes(z)) +
  stat_ecdf(data=data) +
  coord_cartesian(xlim=c(0, 10000))



ggplot(sqldf('
select z,count(*) as count,DENSE_RANK() OVER (ORDER BY count(*) DESC) AS rank
from data
group by z
order by count(*) DESC
'))+
  geom_line(aes(rank,count/length(data$z)))
coord_cartesian(xlim=c(0, 100))




