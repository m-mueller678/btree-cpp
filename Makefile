sources= *.cpp *.hpp

all: test.elf optimized.elf

test.elf: $(sources)
	clang++ btree2020.cpp test.cpp -g -o test.elf

optimized.elf: $(sources)
	clang++ btree2020.cpp test.cpp -O3 -march=native -DNDEBUG -o optimized.elf

clean:
	rm test.elf optimized.elf