JOBS=1

python3 R/eval-2/re-eval.py|shuf |parallel -j$JOBS --timeout 600 --joblog joblog-re-eval -- {1}| tee R/eval-2/re-eval.csv
python3 R/in-memory-skew/skew-op2.py|shuf |parallel -j$JOBS --timeout 600 --joblog joblog-skew -- {1}| tee R/in-memory-skew/skew-op2.csv
python3 R/size3/vary6.py|shuf|parallel -j$JOBS --timeout 600 --joblog joblog-v6 -- {1}| tee R/size3/vary6.csv
python3 R/eval-dense/var-density-new-op.py|shuf|parallel -j$JOBS --timeout 600 --joblog joblog-var-density -- {1}| tee R/eval-dense/var-density-new-op.csv
python3 R/eval-dense/dense-tasks-op2.py |shuf|parallel -j$JOBS --timeout 600 --joblog joblog-dense -- {1}| tee R/eval-dense/dense-tasks-op2.csv
