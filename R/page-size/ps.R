source('../common.R')

# old run with j4 and no ycsb-d
# parallel -j1 --joblog joblog -- env YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=5e7 DATA=int KEY_COUNT=25000000 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 4 5 ::: page-size-builds/*/*-n3-ycsb > pagesize.csv
r = read.csv('d1.csv', strip.white = TRUE)


r <- r %>%
  mutate(avg_key_size = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))

r$page_size <- r$const_pageSize
r <- r %>% select(-starts_with("const"))
r$config_name <- ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


d <- sqldf('
select * from r
where true
and config_name!="art"
and op in ("ycsb_c","ycsb_c_init","ycsb_d","ycsb_e")
')
d <- sqldf('select d.*,d2.* from d join d as d2 using (run_id,op,data_name,config_name) where d2.page_size=4096')
names(d)[(ncol(d) - ncol(d)/2 + 1):ncol(d)] <- paste("s0_", tail(names(d), ncol(d)/2), sep = "")

ggplot(d) +
  scale_color_hue()+
  facet_nested(op~data_name, scales = 'free') +
  geom_line(aes(page_size, scale / time,col=config_name), stat = "summary", fun = mean) +
  expand_limits(y = 0) +
  scale_y_continuous(labels = label_number(scale_cut = cut_si("")), expand = c(0, 1e5)) +
  scale_x_log10(
    breaks = 2^seq(1,20),
    labels = label_number(scale_cut = cut_si("B"))
  )+
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

