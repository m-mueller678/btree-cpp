sources= *.cpp *.hpp
cpps=btree2020.cpp test.cpp dense.cpp hash.cpp
cc=clang++-15 -std=c++17

all: asan.elf test.elf optimized.elf

asan.elf: $(sources)
	$(cc) -fsanitize=address $(cpps) -g -o asan.elf

test.elf: $(sources)
	$(cc) $(cpps) -g -o test.elf -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	$(cc) $(cpps) -O3 -march=native -DNDEBUG -g -o optimized.elf

clean:
	rm test.elf optimized.elf asan.elf
