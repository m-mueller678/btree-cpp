source('../common.R')

# awk 'BEGIN{for(i=8;i<16;++i) for(l=8;l<16;++l) printf("-DPS_I=%d -DPS_L=%d\n",2^i,2^l)}' | ./build_var_page_size.sh
# rm page-size-builds/*/art-*
r = rbind(
  # parallel -j16 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e6 DATA=int KEY_COUNT=2.5e6 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 4 5 ::: page-size-builds/*/*-n3-ycsb > pagesize-f.csv
  # r = read.csv('d2-f.csv', strip.white = TRUE)
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 DATA=int KEY_COUNT=25e6 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 4 5 ::: page-size-builds/*/*-n3-ycsb > pagesize.csv
  read.csv('d2.csv', strip.white = TRUE),
  # removed all builds with 256B sizes
  # during run removed all with 512B inner sizes, as they cause crashes and han\gs
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 DATA=data/urls KEY_COUNT=4268639 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 4 5 ::: page-size-builds/*/*-n3-ycsb > pagesize.csv
  read.csv('d2url.csv', strip.white = TRUE),
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 DATA=data/wiki KEY_COUNT=9818360 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 4 5 ::: page-size-builds/*/*-n3-ycsb > pagesize-wiki.csv
  read.csv('d2wiki.csv', strip.white = TRUE)
  # parallel -j1 --joblog joblog -- YCSB_VARIANT={2} SCAN_LENGTH=100 RUN_ID={1} OP_COUNT=1e7 DATA=data/urls KEY_COUNT=4268639 PAYLOAD_SIZE=8 ZIPF=-1 {3} ::: $(seq 1 1000) ::: 3 5 ::: page-size-builds/*/*-n3-ycsb > pagesize-urls2.csv

)

r <- r %>%
  mutate(avg_key_size = case_when(
    data_name == 'data/urls' ~ 62.280,
    data_name == 'data/wiki' ~ 22.555,
    data_name == 'data/access' ~ 125.54,
    data_name == 'data/genome' ~ 9,
    data_name == 'int' ~ 4,
    TRUE ~ NA
  ))

r$psi <- log2(r$const_pageSizeInner)
r$psl <- log2(r$const_pageSizeLeaf)
r <- r %>% select(-starts_with("const"))
r$config_name <- ordered(r$config_name, levels = CONFIG_NAMES, labels = CONFIG_NAMES)


d <- sqldf('
select * from r
where true
and config_name!="art"
and op in ("ycsb_c","ycsb_c_init","ycsb_e")
')


ggplot(sqldf('select psi,psl,op,data_name,config_name, avg(scale/time) as z from d group by psi,psl,op,data_name,config_name')) +
  scale_color_hue() +
  facet_nested(config_name ~ data_name + op, scales = 'free') +
  geom_raster(aes(x = psi, y = psl, fill = z)) +
  scale_fill_continuous()

{
  d2 <- d|>
    filter((psl >= 10 & psi >= 10) | data_name == 'int')|>
    filter(! (config_name %in% c('baseline','prefix','inner')))
  #|>mutate(time = time / case_when(op == 'ycsb_e' ~ 3, TRUE ~ 1))

  d2 <- rbind(
    d2 |> filter(psl == 12)|> mutate(kind='psi', x=psi),
    d2 |> filter(psi == 12)|> mutate(kind='psl', x=psl)
  )

    ggplot(d2) +
      facet_nested(op ~ config_name,scales = 'free_y') +
      geom_line(aes(x = x, y = time / scale, col = data_name,linetype=kind), stat = 'summary', fun = mean) +
    scale_fill_continuous() +
    #expand_limits(y = 0) +
    scale_y_continuous(
      labels = label_number(scale_cut = cut_si("s")),
      name = "Time (Point)",
       #sec.axis = sec_axis(trans = ~ .x * 3,
       #                    labels = label_number(scale_cut = cut_si("s")),
       #                    name = "Time (Scan)")
    ) +
      scale_linetype_manual(
        values=c(psl='solid',psi='dashed'),
        labels = c(psl='leaf size',psi='inner size'),
        name='variable'
      )+
      xlab('node size')+
    #coord_cartesian(ylim = c(0, 1.0e-6))+
      theme(legend.position = "bottom")
}

d_psi <- d %>%
  pivot_wider(id_cols = any_of(setdiff(frame_id_cols(d), c('bin_name', 'run_id'))), names_from = psi, names_prefix = 'psi', values_from = time, values_fn = mean)

print(d_psi %>%
        filter(psl == 12) %>%
        mutate(r12 = psi11 / psi12 - 1) %>%
        mutate(r13 = psi12 / psi13 - 1) %>%
        mutate(r14 = psi13 / psi14 - 1) %>%
        mutate(r15 = psi14 / psi15 - 1) %>%
        arrange(data_name, op, config_name),
      n = 1000
)


ggplot(sqldf('select * from d where psl==12 and psi>=12 and not config_name in ("baseline","prefix","heads")')) +
  facet_nested(psl ~ data_name + op, scales = 'free') +
  geom_line(aes(x = psi, y = instr, col = config_name), stat = 'summary', fun = mean) +
  scale_color_hue() +
  expand_limits(y = 0)

d|>
  #filter(config_name %in% c('heads','hints','inner'))|>
  ggplot() +
  facet_nested(op ~ data_name + psi, scales = 'free') +
  geom_line(aes(x = psl, y = time / scale, col = config_name), stat = 'summary', fun = mean) +
  scale_color_hue() +
  expand_limits(y = 0)

d|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = scale / time)|>
  group_by(data_name, op)|>
  slice_max(order_by = tp)|>
  #pivot_longer(cols=c(psi,psl),names_to='kind')|>
  ggplot() +
  geom_point(aes(data_name, op, col = factor(psl), fill = factor(psi), shape = config_name), size = 5, stroke = 5) +
  scale_color_viridis_d(aesthetics = c("colour", "fill")) +
  scale_shape_manual(values = 21:24) +
  guides(fill = guide_legend(override.aes = list(shape = 21, stroke = 1)),
         col = guide_legend(override.aes = list(shape = 21, size = 3, stroke = 3)),
         shape = guide_legend(override.aes = list(stroke = 1))
  )

d|>
  filter(config_name == 'hints')|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = scale / time)|>
  group_by(data_name, op)|>
  slice_max(order_by = tp, n = 1)|>
  pivot_longer(cols = c(psi, psl), names_to = 'kind')|>
  ggplot() +
  facet_wrap(~op) +
  geom_col(aes(x = data_name, y = value, group = kind, fill = kind), position = 'dodge')

d|>
  group_by(config_name, data_name, op, psl, psi)|>
  summarise(tp = scale / time)|>
  group_by(data_name, op, config_name)|>
  slice_max(order_by = tp, n = 1, with_ties = FALSE)|>
  # pivot_longer(cols=c(psi,psl),names_to='kind')|>
  ggplot() +
  facet_wrap(~config_name) +
  geom_point(aes(x = psl, y = psi, shape = op, col = data_name, size = tp), stroke = 2, alpha = 0.3) +
  scale_shape_manual(values = 21:24)

d|>
  filter(!is.na(scale / time))|>
  group_by(config_name, data_name, op)|>
  summarise(psl = weighted.mean(psl, (scale / time)^2),
            psi = weighted.mean(psi, (scale / time)^2))|>
  # pivot_longer(cols=c(psi,psl),names_to='kind')|>
  ggplot() +
  facet_wrap(~config_name) +
  geom_point(aes(x = psl, y = psi, shape = op, col = data_name), stroke = 2) +
  scale_shape_manual(values = 21:24)


ggplot(sqldf('
select *
from d
where true
--and not config_name in ("dense1", "dense2")
')) +
  facet_nested(psi ~ data_name + psl) +
  geom_bar(aes(x = config_name, y = scale / time, fill = config_name), stat = 'summary', fun = mean) +
  scale_color_hue() +
  scale_fill_hue() +
  expand_limits(y = 0)