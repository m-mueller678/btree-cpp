library(ggplot2)
library(ggh4x)
library(dplyr)

file_paths <- c("kl_int.gz", "kl_rng4.gz", "kl_wiki.gz", "kl_urls.gz")

process_file <- function(file_path) {
  con <- gzfile(file_path, "rt")
  lines <- readLines(con)
  close(con)

  values <- unlist(lapply(lines, function(line) {
    numbers <- strsplit(gsub("^kl", "", line), " ")[[1]]
    as.integer(numbers)
  }))
  data.frame(
    values = values,
    filename = basename(file_path)  # Just the file name, not the full path
  )
}

df <- do.call(rbind, lapply(file_paths, process_file))|>transmute(name = filename, length = values)

df|>
  mutate(length = ifelse(length>60,70,length))|>
  mutate(length = replace_na(length, 71))|>
  group_by(name, length)|>
  count()|>
  group_by(name)|>
  mutate(cs = cumsum(n),rel = cs/sum(n))|>
  ggplot() +
  facet_nested(name ~ ., scales = 'free') +
  geom_col(aes(x = length, y = rel))

