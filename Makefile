sources= *.cpp *.hpp
cpps=btree2020.cpp test.cpp dense.cpp hash.cpp

all: asan.elf test.elf optimized.elf

asan.elf: $(sources)
	clang++ -fsanitize=address $(cpps) -g -o asan.elf

test.elf: $(sources)
	clang++ $(cpps) -g -o test.elf -Wall -Wextra -Wpedantic

optimized.elf: $(sources)
	clang++ $(cpps) -O3 -march=native -DNDEBUG -o optimized.elf

clean:
	rm test.elf optimized.elf asan.elf