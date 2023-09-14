library(ggplot2)
library(sqldf)
library(ggh4x)
library(dplyr)
library(stringr)
library(scales)


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

fetch2 <- function(d,keyCol,keyA,keyB,group,metric) {
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

fetch2Relative <- function(d,keyCol,keyA,keyB,group,metric){
  x <- fetch2(d,keyCol,keyA,keyB,group,metric)
  return(fn$sqldf('select $group,b/a -1 as r from x order by $group'))
}

extremesBy <-function (r,d){
  d[c(which.min(d[,r]),which.max(d[,r])),]
}

CONFIG_NAMES <- c('baseline', 'prefix', 'heads', 'hints', 'inner', 'hash', 'dense', 'dense1', 'dense2', 'art')
