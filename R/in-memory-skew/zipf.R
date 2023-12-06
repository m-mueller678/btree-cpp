source('../common.R')

n <- 10000000     # Number of data points
zipf_data <- function(s) {
  x <- seq(1, n)
  r <- data.frame(rank = x, frequency = 1 / (x^s))
  r$zipf_cdf <- cumsum(r$frequency)
  r$zipf_cdf <- r$zipf_cdf / max(r$zipf_cdf)
  r <- (r %>% mutate(s = s))
  r$s <- factor(r$s)
  return(r)
}

data <- rbind(
  zipf_data(0),
  zipf_data(0.5),
  zipf_data(0.75),
  zipf_data(1.0),
  zipf_data(1.25),
  zipf_data(1.5)
)

{ # zipf cdf

  #ggplot(data) +
  ggplot(data %>% sample_n(100000)) +theme_bw()+
    scale_y_continuous(labels = label_percent(),name=NULL,limits = c(0,1.1),expand = expansion(0))+
    scale_x_continuous(labels = label_percent(),name='Key Percentile',limits = c(0,1),expand = expansion(0))+
    scale_color_brewer(palette = 'Dark2',name = "α") +
    theme(
      legend.position = 'right',
      legend.margin=margin(20,0,0,5)
    ) +
    guides(col = guide_legend(ncol = 2,title.position="top", title.hjust = 0.5))+
    geom_line(aes(x = rank/n, y = zipf_cdf, col = s)) +
    ylab(NULL)
}
save_as('zipf-distribution',30)

