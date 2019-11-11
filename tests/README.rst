To avoid troubles with pickling non-`__main__` classes all real test cases are
spread though `run_*.py` files. Therefore they are not picked by `pytest` and run separately.