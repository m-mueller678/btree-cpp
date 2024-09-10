hot_includes=$(shell find in-memory-structures/hot/ -type d -name include | sed 's/^/-I/')
zipf_sources = $(zipfc/Cargo.toml zipfc/Cargo.lock zipfc/src/lib.rs)
core_cpps=$(shell find btree -type f|grep cpp|grep -v hot|grep -v lits) in-memory-structures/ART/ART.cpp
cpp_sources=$(core_cpps) tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*.hpp test.cpp ycsb2.cpp
sources= $(cpp_sources) $(zipf_sources)
test_cpps=test.cpp $(core_cpps)
tpcc_cpps=tpcc/newbm.cpp $(core_cpps)
hot_sources=$(shell find in-memory-structures/hot/ -type f) btree/hot_adapter.*
lits_sources=$(shell find in-memory-structures/lits/ -type f) btree/lits_adapter.*
ycsb_cpps=ycsb2.cpp $(core_cpps)
cxx_base=/usr/bin/clang++-15
cc_base=/usr/bin/clang-15
cxx=$(cxx_base) $(PAGE_SIZE_OVERRIDE_FLAG) -std=c++17 -o $@ -march=native -g

zipfc_link_arg = -Lzipfc/target/release/ -lzipfc -L. -lwh
named_config_headers = $(shell ls named-configs)
config_names = $(named_config_headers:%.hpp=%)
named_tpcc_d3_builds = $(config_names:%=named-build/%-d3-tpcc)
named_tpcc_n3_builds = $(config_names:%=named-build/%-n3-tpcc)
named_test_d3_builds = $(config_names:%=named-build/%-d3-test)
named_test_n3_builds = $(config_names:%=named-build/%-n3-test)
named_test_d0_builds = $(config_names:%=named-build/%-d0-test)
named_ycsb_n3_builds = $(config_names:%=named-build/%-n3-ycsb)
named_ycsb_d0_builds = $(config_names:%=named-build/%-d0-ycsb)
named_ycsb_d3_builds = $(config_names:%=named-build/%-d3-ycsb)
named_builds = $(named_tpcc_d3_builds) $(named_tpcc_n3_builds) $(named_test_d3_builds) $(named_test_n3_builds) $(named_test_d0_builds) $(named_ycsb_n3_builds) $(named_ycsb_d0_builds) $(named_ycsb_d3_builds)
named_args = -include named-configs/$*.hpp -DNAMED_CONFIG=\"$*\"

all: test.elf optimized.elf $(named_builds)

ycsb-all: $(named_ycsb_n3_builds) $(named_ycsb_d0_builds) $(named_ycsb_d3_builds)

asan.elf: $(sources)
	$(cxx) -fsanitize=address $(test_cpps)

test.elf: $(sources)
	$(cxx) $(test_cpps) -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	$(cxx) $(test_cpps) -O3  -DNDEBUG

stream.elf: $(sources) stream.cpp
	$(cxx) stream.cpp -O3  -DNDEBUG

clean:
	rm -f test.elf optimized.elf asan.elf tpcc-optimzed.elf tpcc-debug.elf
	rm -rf named-build
	rm -rf leanstore/build

format:
	clang-format -i $(cpp_sources) btree/hot_adapter.*

tpcc-optimzed.elf: $(sources)
	$(cxx) $(tpcc_cpps) -O3  -DNDEBUG  -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb

tpcc-debug.elf: $(sources)
	$(cxx) $(tpcc_cpps)   -ltbb  -Wall -Wextra -Wpedantic

zipfc/target/release/libzipfc.a: zipfc/Cargo.toml zipfc/Cargo.lock zipfc/src/lib.rs
	cd zipfc; cargo build --release

#### named tpcc

named-build/%-n3-tpcc: hot-n3.o lits-n3.o libwh.so $(sources)
	@mkdir -p named-build
	$(cxx) $(tpcc_cpps) -O3  -DNDEBUG  -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args) hot-n3.o lits-n3.o

named-build/%-d3-tpcc: hot-d0.o lits-d0.o libwh.so $(sources)
	@mkdir -p named-build
	$(cxx) $(tpcc_cpps) -O3 -fnon-call-exceptions -fasynchronous-unwind-tables -ltbb $(named_args) hot-d0.o lits-d0.o

#### named test

named-build/%-d0-test: hot-d0.o lits-d0.o libwh.so $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) $(named_args) hot-d0.o lits-d0.o

named-build/%-d3-test: hot-d0.o lits-d0.o libwh.so $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) -O3 $(named_args) hot-d0.o lits-d0.o

named-build/%-n3-test: hot-n3.o lits-n3.o libwh.so $(sources)
	@mkdir -p named-build
	$(cxx) $(test_cpps) -O3 -DNDEBUG $(named_args) hot-n3.o lits-n3.o

#### named ycsb
named-build/%-n3-ycsb: $(sources) hot-n3.o lits-n3.o tlx_build_n3/libTlxWrapper.a zipfc/target/release/libzipfc.a libwh.so
	@mkdir -p named-build
	$(cxx) $(ycsb_cpps) -O3 -DNDEBUG $(named_args) $(zipfc_link_arg) -Ltlx_build_n3/ -lTlxWrapper hot-n3.o lits-n3.o

named-build/%-d0-ycsb: $(sources) hot-d0.o lits-d0.o tlx_build_d0/libTlxWrapper.a zipfc/target/release/libzipfc.a libwh.so
	@mkdir -p named-build
	$(cxx) $(ycsb_cpps) $(named_args) $(zipfc_link_arg) -Ltlx_build_d0/ -lTlxWrapper hot-d0.o lits-d0.o


named-build/%-d3-ycsb: $(sources) hot-d0.o lits-d0.o tlx_build_d0/libTlxWrapper.a zipfc/target/release/libzipfc.a libwh.so
	@mkdir -p named-build
	$(cxx) $(ycsb_cpps) -O3 $(named_args) $(zipfc_link_arg) -Ltlx_build_d0/ -lTlxWrapper hot-d0.o lits-d0.o

hot-d0.o: $(hot_sources)
	g++-11 -c btree/hot_adapter.cpp -std=c++17 -o $@ -march=native -g $(hot_includes)

hot-n3.o: $(hot_sources)
	g++-11 -c  -DNDEBUG -O3 btree/hot_adapter.cpp -std=c++17 -o $@ -march=native -g $(hot_includes)

lits-d0.o: $(lits_sources)
	g++-11 -c btree/lits_adapter.cpp -std=c++17 -o $@ -march=native -g

lits-n3.o: $(lits_sources)
	g++-11 -c  -DNDEBUG -O3 btree/lits_adapter.cpp -std=c++17 -o $@ -march=native -g

tlx_build_d0/libTlxWrapper.a: .FORCE
	mkdir -p tlx_build_d0
	cd tlx_build_d0; cmake -DCMAKE_BUILD_TYPE=Debug ../tlx_wrapper; cmake --build .

tlx_build_n3/libTlxWrapper.a: .FORCE
	mkdir -p tlx_build_n3
	cd tlx_build_n3; cmake -DCMAKE_BUILD_TYPE=Release ../tlx_wrapper/; cmake --build . --config Release

libwh.so:
	cd in-memory-structures/wormhole; make libwh.so
	cp in-memory-structures/wormhole/libwh.so .

#### phony

.PHONY: debug-named-tpcc
debug-named-tpcc: $(named_tpcc_d3_builds)
	parallel --delay 0.5 --memfree 16G -j75% -q -- env RUNFOR=15 WH=5 {1} ::: $(named_tpcc_d3_builds)

.PHONY: tpcc
tpcc: $(named_tpcc_n3_builds)
	parallel --memfree 16G -q -- {1} ::: $(named_tpcc_n3_builds) | awk '/const/ {if(!header)print;header=1} !/const/' > tpcc.csv

.PHONY: .FORCE
.FORCE: