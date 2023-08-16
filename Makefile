sources= btree/*.?pp tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*.hpp test.cpp
core_cpps=btree/*.cpp
test_cpps=test.cpp $(core_cpps)
tpcc_cpps=tpcc/newbm.cpp $(core_cpps)
cxx_base=/usr/bin/clang++-15
cc_base=/usr/bin/clang-15
cxx=$(cxx_base) -std=c++17 -o $@ -march=native -g

named_config_headers = $(shell ls named-configs)
config_names = $(named_config_headers:%.hpp=%)
named_tpcc_d3_builds = $(config_names:%=named-build/%-d3-tpcc)
named_tpcc_n3_builds = $(config_names:%=named-build/%-n3-tpcc)
named_test_d3_builds = $(config_names:%=named-build/%-d3-test)
named_test_n3_builds = $(config_names:%=named-build/%-n3-test)
named_test_d0_builds = $(config_names:%=named-build/%-d0-test)
named_builds = $(named_tpcc_d3_builds) $(named_tpcc_n3_builds) $(named_test_d3_builds) $(named_test_n3_builds) $(named_test_d0_builds)
named_args = -include named-configs/$*.hpp -DNAMED_CONFIG=\"$*\"

ycsb_binaries = $(config_names:%=leanstore/build/frontend/btree_cpp_ycsb_%)

all: test.elf optimized.elf $(named_builds)

asan.elf: $(sources)
	$(cxx) -fsanitize=address $(test_cpps)

test.elf: $(sources)
	$(cxx) $(test_cpps) -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	$(cxx) $(test_cpps) -O3  -DNDEBUG

clean:
	rm -f test.elf optimized.elf asan.elf tpcc-optimzed.elf tpcc-debug.elf
	rm -rf named-build
	rm -rf leanstore/build

format:
	clang-format -i $(sources)

tpcc-optimzed.elf: $(sources)
	$(cxx) $(tpcc_cpps) -O3  -DNDEBUG  -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb

tpcc-debug.elf: $(sources)
	$(cxx) $(tpcc_cpps)   -ltbb  -Wall -Wextra -Wpedantic

#### named tpcc

named-build/%-n3-tpcc: $(sources)
	@mkdir -p named-build
	$(cxx) $(tpcc_cpps) -O3  -DNDEBUG  -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args)

named-build/%-d3-tpcc: $(sources)
	@mkdir -p named-build
	$(cxx) $(tpcc_cpps) -O3 -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args)

#### named test

named-build/%-d0-test: $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) $(named_args)

named-build/%-d3-test: $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) -O3 $(named_args)

named-build/%-n3-test: $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) -O3 -DNDEBUG $(named_args)

#### phony

.PHONY: debug-named-tpcc
debug-named-tpcc: $(named_tpcc_d3_builds)
	parallel --delay 0.5 --memfree 16G -j75% -q -- env RUNFOR=15 WH=5 {1} ::: $(named_tpcc_d3_builds)


.PHONY: tpcc
tpcc: $(named_tpcc_n3_builds)
	parallel --memfree 16G -q -- {1} ::: $(named_tpcc_n3_builds) | awk '/const/ {if(!header)print;header=1} !/const/' > tpcc.csv

.PHONY: ycsb
ycsb:
	git submodule update --remote --init leanstore/
	@mkdir -p leanstore/build
	cd leanstore/build; cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -D CMAKE_C_COMPILER=$(cc_base) -D CMAKE_CXX_COMPILER=$(cxx_base) ..
	cd leanstore/build; make $(config_names:%=btree_cpp_ycsb_%) btree_cpp_ycsb
	parallel --memfree 16G -q -- {1} --ycsb_tuple_count 10000000  --run_for_seconds 30 \$RUNFOR ::: $(ycsb_binaries) | awk '/const/ {if(!header)print;header=1} !/const/' > ycsb.csv
