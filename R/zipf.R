library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

{ #zipf probabilities
  set.seed(123) # for reproducibility
  n <- 25000000     # Number of data points
  s <- 0.5        # Shape parameter (you can adjust this)
  x <- seq(1, n,n/1000000)
  zipf_data <- data.frame(rank = x, frequency = 1 / (x^s))

  ggplot(zipf_data, aes(x = rank, y = frequency)) +
    geom_line() +
    #scale_x_continuous(trans = "log10", breaks = c(1, 10, 100, 1000)) +
    #scale_y_continuous(trans = "log10") +
    scale_x_continuous(limits =c(0,1000))

}

{# zipf cdf
  set.seed(123) # for reproducibility
  n <- 25000000     # Number of data points
  s <- 0.6        # Shape parameter (you can adjust this)
  x <- seq(1,n)
  zipf_data <- data.frame(rank = x, frequency = 1 / (x^s))
  zipf_data$zipf_cdf <- cumsum(zipf_data$frequency)
  zipf_data$zipf_cdf <- zipf_data$zipf_cdf/max(zipf_data$zipf_cdf)
  ggplot(zipf_data %>% sample_n(10000)) +
    geom_line(aes(x = rank, y = zipf_cdf))
}



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






