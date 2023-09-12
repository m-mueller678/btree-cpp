source('../common.R')

r <- read.csv('d1.csv', strip.white = TRUE) %>% mutate(sequential = TRUE)

ggplot(r) +
  geom_line(aes(size, time / scale)) +
  expand_limits(y = 0) +
  scale_x_continuous(labels = label_number(scale_cut = cut_si("B"))) +
  scale_y_continuous(labels = label_number(scale_cut = cut_si("s")))

