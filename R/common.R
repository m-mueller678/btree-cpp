library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)
library(stringr)
library(scales)
library(tidyr)
library(patchwork)
library(forcats)
library(readr)
library(RColorBrewer)
library(ggpattern)

format_si <- function(...) {
  # https://stackoverflow.com/a/21089837
  # Based on code by Ben Tupper
  # https://stat.ethz.ch/pipermail/r-help/2012-January/299804.html

  function(x) {
    limits <- c(1e-24, 1e-21, 1e-18, 1e-15, 1e-12,
                1e-9, 1e-6, 1e-3, 1e0, 1e3,
                1e6, 1e9, 1e12, 1e15, 1e18,
                1e21, 1e24)
    prefix <- c("y", "z", "a", "f", "p",
                "n", "µ", "m", " ", "k",
                "M", "G", "T", "P", "E",
                "Z", "Y")

    # Vector with array indices according to position in intervals
    i <- findInterval(abs(x), limits)

    # Set prefix to " " for very small values < 1e-24
    i <- ifelse(i == 0, which(limits == 1e0), i)

    paste(format(round(x / limits[i], 1),
                 trim = TRUE, scientific = FALSE, ...),
          prefix[i])
  }
}

fetch2 <- function(d, keyCol, keyA, keyB, group, metric) {
  counts <- fn$sqldf('select $group,$keyCol,count(*) as count from d group by $group,$keyCol')
  print(fn$sqldf('select min(count),max(count) from counts'))
  return(fn$sqldf('
  select $group,a,b from (
    select $group,$keyCol,x as b,lag(x,1) over win as a,rank() over win as rank
    from (
      select $group,$keyCol,avg($metric) as x
      from d group by $group,$keyCol
    )
    where $keyCol in ("$keyA","$keyB")
    window win  as (partition by $group order by $keyCol=="$keyB")
  )
  where rank=2
  order by $group'))
}

fetch2Relative <- function(d, keyCol, keyA, keyB, group, metric) {
  x <- fetch2(d, keyCol, keyA, keyB, group, metric)
  return(fn$sqldf('select $group,b/a -1 as r from x order by $group'))
}

extremesBy <- function(r, d) {
  d[c(which.min(d[, r]), which.max(d[, r])),]
}

VAL_COLS = c("time", "cycle", "instr", "L1_miss", "LLC_miss", "br_miss", "IPC", "CPU", "GHz", "task")
frame_id_cols <- function(c) setdiff(colnames(c), VAL_COLS)

DATA_MAP <- c('data/urls' = 'urls-full', 'data/urls-short' = 'urls', 'data/wiki' = 'wiki', 'rng8' = 'sparse64', 'rng4' = 'sparse', 'partitioned_id' = 'partitioned_id', 'int' = 'ints')
DATA_LABELS <- c('urls-full' = 'urls-full', 'urls' = 'urls', 'wiki' = 'wiki', 'sparse64' = 'sparse64', 'sparse' = 'sparse', 'partitioned_id' = 'partitioned_id', 'ints' = 'dense')

BASIC_OPTS <- c('prefix', 'heads', 'hints')
OP_LABELS <- c('ycsb_c' = 'lookup', 'ycsb_c_init' = 'insert0', 'ycsb_e_init' = 'ycsb_e_init', 'sorted_scan_init' = 'sorted_scan_init', 'sorted_insert' = 'sorted insert', 'insert90' = 'insert', 'sorted_scan' = 'warm scan', 'scan' = 'scan', 'ycsb_e' = 'YCSB-E', 'hack_dense_leaf_share'='dense leaf share')
CONFIG_LABELS <- c('baseline' = 'baseline',
                   'prefix' = 'prefix truncation',
                   'heads' = 'heads',
                   'hints' = 'hints',
                   'inner' = 'integer separators',
                   'hash' = 'fingerprinting',
                   'dense1' = 'no split',
                   'dense2' = 'SDLs',
                   'dense3' = 'FDLs',
                   'adapt' = 'ket-adaptive',
                   'adapt2' = 'adaptive',
                   'art' = 'ART',
                   'hot' = 'HOT',
                   'tlx' = 'TLX',
                   'wh' = 'WH',
                   'lits'='LITS',
                   'lits2'='LITS2'
)
CONFIG_NAMES <- names(CONFIG_LABELS)

config_reference <- function(d) {
  case_when(
    d == 'baseline' ~ NA,
    d == 'prefix' ~ 'baseline',
    d == 'heads' ~ 'prefix',
    d == 'hints' ~ 'heads',
    TRUE ~ 'hints'
  )
}


augment <- function(d) {
  d|>
    mutate(
      psi = log2(const_pageSizeInner),
      psl = log2(const_pageSizeLeaf),
      total_leaf_prefix = if ('total_leaf_prefix' %in% colnames(d)) { total_leaf_prefix }else { NA },
      avg_key_size = case_when(
        data_name == 'data/urls' ~ 62.280,
        data_name == 'data/urls-short' ~ 62.204,
        data_name == 'data/wiki' ~ 22.555,
        data_name == 'data/access' ~ 125.54,
        data_name == 'data/genome' ~ 9,
        data_name == 'int' ~ 4,
        data_name == 'rng4' ~ 4,
        data_name == 'rng8' ~ 8,
        TRUE ~ NA
      ),
      data_name = factor(data_name, levels = names(DATA_MAP), labels = DATA_MAP),
      op = factor(op, names(OP_LABELS)),
      # final_key_count = case_when(
      #   op == 'ycsb_c' | op == 'ycsb_c_init' ~ data_size,
      #   op == 'ycsb_e' ~ data_size + scale * 0.025,
      # ),
      final_key_count = if ('counted_final_key_count' %in% colnames(d)) { counted_final_key_count }else { NA },
      leaf_count = nodeCount_Leaf +
        nodeCount_Hash +
        nodeCount_Dense +
        nodeCount_Dense2,
      inner_count = nodeCount_Inner +
        nodeCount_Head4 +
        nodeCount_Head8,
      node_count = leaf_count + inner_count,
      keys_per_leaf = final_key_count / leaf_count,
      total_size = data_size * (avg_key_size + payload_size),
      avg_leaf_prefix = total_leaf_prefix / leaf_count,
      config_name = factor(config_name, levels = CONFIG_NAMES),
      txs = scale / time,
    )|>
    select(-starts_with("const"))
}

OUTPUT_COLS <- c("time", "nodeCount_Leaf", "nodeCount_Inner",
                 "nodeCount_Dense", "nodeCount_Hash", "nodeCount_Head4", "nodeCount_Head8", "nodeCount_Dense2",
                 "counted_final_key_count", "cycle", "instr", "L1_miss", "LLC_miss",
                 "br_miss", "task", "IPC", "CPU",
                 "GHz", "psi", "psl", "avg_key_size", "final_key_count",
                 "leaf_count", "inner_count", "node_count", "keys_per_leaf", "rand_seed", "txs", "total_leaf_prefix", "avg_leaf_prefix"
)

label_page_size <- function(x) {
  ifelse(2^x < 1024, paste(2^x, "B"), scales::label_bytes(units = 'auto_binary')(2^x))
}

read_broken_csv <- function(path) {
  data <- read.csv(path, strip.white = TRUE)
  data <- data[data[[1]] != colnames(data)[1],]
  tibble(data.frame(lapply(data, type.convert, as.is = TRUE)))
}

save_as <- function(name, h, w = 85) {
  ggsave(path = '~/develop/btree-paper/fig/', device = 'svg', filename = paste0(name, '.svg'), width = w, units = 'mm', height = h)
  plot
}