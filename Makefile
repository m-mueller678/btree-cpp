sources= *.?pp tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*
core_cpps=btree2020.cpp dense.cpp hash.cpp anynode.cpp
test_cpps=test.cpp $(core_cpps)
tpcc_cpps=tpcc/newbm.cpp $(core_cpps)
cc=clang++-15 -std=c++17
named_config_headers = $(shell ls named-configs)
config_names = $(named_config_headers:%.hpp=%)
named_builds = $(config_names:%=named-build/%)

all: asan.elf test.elf optimized.elf $(named_builds)

asan.elf: $(sources)
	$(cc) -fsanitize=address $(test_cpps) -g -o asan.elf

test.elf: $(sources)
	$(cc) $(test_cpps) -g -o test.elf -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	$(cc) $(test_cpps) -O3 -march=native -DNDEBUG -g -o optimized.elf

clean:
	rm -f test.elf optimized.elf asan.elf tpcc-optimzed.elf tpcc-debug.elf
	rm -rf named-build

format:
	clang-format -i *.?pp tpcc/*.?pp tpcc/tpcc/*.?pp named-configs/*

tpcc-optimzed.elf: $(sources)
	$(cc) $(tpcc_cpps) -O3 -march=native -DNDEBUG -g -fnon-call-exceptions -fasynchronous-unwind-tables  -o tpcc-optimzed.elf -ltbb

tpcc-debug.elf: $(sources)
	$(cc) $(tpcc_cpps) -g -o tpcc-debug.elf -ltbb  -Wall -Wextra -Wpedantic

named-build/%:
	mkdir -p named-build
	$(cc) $(tpcc_cpps) -O3 -march=native -DNDEBUG -g -fnon-call-exceptions -fasynchronous-unwind-tables  -o named-build/$* -ltbb -include named-configs/$*.hpp -DNAMED_CONFIG=\"$*\"

.PHONY: always-rebuild
