library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)

{ #zipf probabilities
  set.seed(123) # for reproducibility
  n <- 25000000     # Number of data points
  s <- 0.5        # Shape parameter (you can adjust this)
  x <- seq(1, n, n / 1000000)
  zipf_data <- data.frame(rank = x, frequency = 1 / (x^s))

  ggplot(zipf_data, aes(x = rank, y = frequency)) +
    geom_line() +
    #scale_x_continuous(trans = "log10", breaks = c(1, 10, 100, 1000)) +
    #scale_y_continuous(trans = "log10") +
    scale_x_continuous(limits = c(0, 1000))

}

n <- 25000000     # Number of data points
zipf_data <- function(s) {
  x <- seq(1, n)
  r <- data.frame(rank = x, frequency = 1 / (x^s))
  r$zipf_cdf <- cumsum(r$frequency)
  r$zipf_cdf <- r$zipf_cdf / max(r$zipf_cdf)
  r <- (r %>% mutate(s = s))
  r$s <- factor(r$s)
  return(r)
}

{ # zipf cdf
  data = rbind(zipf_data(0), zipf_data(0.5), zipf_data(0.75), zipf_data(1.0), zipf_data(1.25), zipf_data(1.5))
  #ggplot(data) +
  ggplot(data %>% sample_n(10000)) +
    scale_color_discrete(name = "α") +
    theme(
      legend.position = c(1, 0.5),
      legend.justification='right',
    ) +
    geom_line(aes(x = rank, y = zipf_cdf, col = s)) +
    ylab(NULL)
}


{
  r <- read.csv('../zipf_samples.csv', header = FALSE)
  colnames(r) <- c('z')
  data <- head(r, n = 10000)

  ggplot() +
    stat_ecdf(data=data, mapping=aes(z))+
    geom_line(data=zipf_data(0.8)%>% sample_n(1000),mapping=aes(x=rank,y=zipf_cdf),col='blue')
}

ggplot(sqldf('
select z,count(*) as count,DENSE_RANK() OVER (ORDER BY count(*) DESC) AS rank
from data
group by z
order by count(*) DESC
')) +
  geom_line(aes(rank, count / length(data$z)))
coord_cartesian(xlim = c(0, 100))






