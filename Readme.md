This repository contains various data structure implementations, benchmarking code, and benchmarking results.

# Benchmarking Binaries
The main artifacts produced are benchmarking binaries produced from the makefile target `ycsb-all`.
Each binary implements benchmarking of one data structure configuration and follows the naming scheme `<config>-<debug_assertions><optimization_level>-ycsb`.
`config` specifies corresponds to one of the configurations in the `named-configs` directory.
There are configurations for the B-Tree with various optimizations enabled, as well as for different in-memory data structures.
Each binary is configured via a set of environment variables, which are processed in `ycsb2.cpp`

# Variable Page size
By default, all B-Trees use a node size of 4KiB.
The 

# R
Each subdirectory of the `R` directory contains python scripts to generate benchmarks, results of said benchmarks as csv files, and R code to analyze the results.
The output of each python script is a sequence of program invocations, intended to be piped into GNU parallel.
The csv file containing the results is generally named after the python script that produced it.