sources= *.cpp *.hpp
cpps=btree2020.cpp test.cpp dense.cpp hash.cpp
cc=clang++-15 -std=c++17 -Wl,-rpath,.  $(crc32cso)
crc32cso='crc32c/build/libcrc32c.so'

all: asan.elf test.elf optimized.elf

asan.elf: $(sources) $(crc32cso)
	$(cc) -fsanitize=address $(cpps) -g -o asan.elf

test.elf: $(sources) $(crc32cso)
	$(cc) $(cpps) -g -o test.elf -Wall -Wextra -Wpedantic

optimized.elf: $(sources) $(crc32cso)
	$(cc) $(cpps) -O3 -march=native -DNDEBUG -g -o optimized.elf

clean:
	rm test.elf optimized.elf asan.elf

$(crc32cso):
	mkdir crc32c/build -p
	cd crc32c/build; cmake -DCRC32C_BUILD_TESTS=0 -DCRC32C_BUILD_BENCHMARKS=0 ..
	cd crc32c/build; make all