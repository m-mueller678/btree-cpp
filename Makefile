sources= *.cpp *.hpp

asan.elf: $(sources)
	clang++ -fsanitize=address btree2020.cpp test.cpp dense.cpp -g -o asan.elf

test.elf: $(sources)
	clang++ btree2020.cpp test.cpp dense.cpp -g -o test.elf

optimized.elf: $(sources)
	clang++ btree2020.cpp test.cpp dense.cpp -O3 -march=native -DNDEBUG -o optimized.elf

clean:
	rm test.elf optimized.elf