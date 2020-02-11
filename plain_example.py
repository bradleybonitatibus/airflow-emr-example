from pyspark import SparkContext
from multiprocessing import Pool, cpu_count
import time
import os 

def calc(x):
    res = (x*x) / 3.4
    print("({0}^2) / 3.4 = {1}".format(x, res))
    return res

if __name__ == "__main__":
    sc = SparkContext(appName="TestEMR")
    start = time.time()
    pool = Pool(cpu_count())
    pool.map(calc, range(5000000))
    dur = time.time()
    print('Execution time was {0} seconds'.format(dur - start))