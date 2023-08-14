sources= *.?pp tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*
core_cpps=btree2020.cpp dense.cpp hash.cpp anynode.cpp
test_cpps=test.cpp $(core_cpps)
tpcc_cpps=tpcc/newbm.cpp $(core_cpps)
cc=clang++-15 -std=c++17 -o $@

named_config_headers = $(shell ls named-configs)
config_names = $(named_config_headers:%.hpp=%)
named_tpcc_debug_builds = $(config_names:%=named-build/%-debug-tpcc)
named_tpcc_opt_builds = $(config_names:%=named-build/%-tpcc)
named_builds =  $(config_names:%=named-build/%-opt-test)  $(named_tpcc_debug_builds) $(named_tpcc_opt_builds)
named_args = -include named-configs/$*.hpp -DNAMED_CONFIG=\"$*\"

all: asan.elf test.elf optimized.elf $(named_builds)

asan.elf: $(sources)
	$(cc) -fsanitize=address $(test_cpps) -g

test.elf: $(sources)
	$(cc) $(test_cpps) -g -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	$(cc) $(test_cpps) -O3 -march=native -DNDEBUG -g

clean:
	rm -f test.elf optimized.elf asan.elf tpcc-optimzed.elf tpcc-debug.elf
	rm -rf named-build

format:
	clang-format -i *.?pp tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*

tpcc-optimzed.elf: $(sources)
	$(cc) $(tpcc_cpps) -O3 -march=native -DNDEBUG -g -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb

tpcc-debug.elf: $(sources)
	$(cc) $(tpcc_cpps) -g  -ltbb  -Wall -Wextra -Wpedantic

named-build/%-tpcc: $(sources)
	mkdir -p named-build
	$(cc) $(tpcc_cpps) -O3 -march=native -DNDEBUG -g -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args)

named-build/%-opt-test: $(sources)
	mkdir -p named-build
	$(cc) $(test_cpps) -O3 -march=native -DNDEBUG -g $(named_args)

named-build/%-debug-tpcc: $(sources)
	mkdir -p named-build
	$(cc) $(tpcc_cpps) -O3 -march=native -g -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args)

debug-named-tpcc: $(named_tpcc_debug_builds)
	parallel --delay 0.5 --memfree 16G -j75% -q -- env RUNFOR=15 WH=5 {1} ::: $(named_tpcc_debug_builds)

.PHONY: tpcc
tpcc: $(named_tpcc_opt_builds)
	parallel --memfree 16G -q -- {1} ::: $(named_tpcc_opt_builds) | awk '/const/ {if(!header)print;header=1} !/const/' > tpcc.csv
