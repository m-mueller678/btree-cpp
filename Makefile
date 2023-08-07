sources= *.cpp *.hpp

test: $(sources)
	clang++ btree2020.cpp test.cpp dense.cpp -g -o test

optimized: $(sources)
	clang++ btree2020.cpp test.cpp dense.cpp -O3 -march=native -DNDEBUG -o optimized

clean:
	rm test