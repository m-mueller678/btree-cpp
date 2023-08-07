sources= *.cpp *.hpp

all: test

test: $(sources)
	clang++ btree2020.cpp test.cpp dense.cpp -g -o test

clean:
	rm test