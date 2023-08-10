sources= *.cpp *.hpp
cpps=btree2020.cpp test.cpp dense.cpp hash.cpp
cc=clang++-15 -std=c++17 -Wl,-rpath,. libcrc32c.so.1

all: asan.elf test.elf optimized.elf

asan.elf: $(sources) libcrc32c.so.1
	$(cc) -fsanitize=address $(cpps) -g -o asan.elf

test.elf: $(sources) libcrc32c.so.1
	$(cc) $(cpps) -g -o test.elf -Wall -Wextra -Wpedantic

optimized.elf: $(sources) libcrc32c.so.1
	$(cc) $(cpps) -O3 -march=native -DNDEBUG -g -o optimized.elf

clean:
	rm -f test.elf optimized.elf asan.elf
	rm -rf crc32c/
	rm -f libcrc32c.so

libcrc32c.so.1:
	git submodule update --init --recursive
	mkdir crc32c/build -p
	cd crc32c/build; cmake -DCRC32C_BUILD_TESTS=0 -DCRC32C_BUILD_BENCHMARKS=0 ..
	cd crc32c/build; make all
	ln -s crc32c/build/libcrc32c.so.1 libcrc32c.so.1