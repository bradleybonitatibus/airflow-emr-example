import random
from pyspark import SparkContext

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

if __name__ == "__main__":
    sc = SparkContext(appName="example_spark_job")
    NUM_SAMPLES = 100000

    count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
    print("Pi is roughly {0}".format((4.0 * count / NUM_SAMPLES)))