from typing import List
from pyspark.sql import SparkSession, DataFrame, Row, Column
import pyspark.sql.functions as F 
from functools import wraps
import time

NUMBER_OF_RUNS = 10

# utility to measure the average runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        total_time = 0
        for _ in range(NUMBER_OF_RUNS):
            t = time.time()
            result = f(*args, **kw)
            total_time += time.time() - t
        avg_time = total_time / NUMBER_OF_RUNS
        print(f'{f.__name__} avg time over {NUMBER_OF_RUNS} runs: {round(avg_time, 5)}s')
        return result
    return wrap

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')

def count_rows(iterator):
    yield len(list(iterator))

@timeit
def q4(bucket_size: int=None, spark=None):

    badges = spark.read.parquet(f"/app/fullDataParquet/badges.parquet").filter(
            (F.col('tagbased') == False) & 
            (F.col('name').isin(
                'Analytical',
                'Census',
                'Documentation Beta',
                'Documentation Pioneer',
                'Documentation User',
                'Reversal',
                'Tumbleweed'
            ) == False) &
            (F.col('class').isin(1, 2, 3)) &
            (F.col('userid') != -1))

    return badges \
        .groupBy(F.window(F.col('date'), f"{bucket_size if bucket_size is not None else 1} seconds")) \
        .agg(F.count('*').alias('count')) \
        .orderBy('window')
                                
def main():
        
    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        
        # .config("spark.sql.shuffle.partitions", "12") \


    q4(bucket_size=600, spark=spark).show()    
    

if __name__ == '__main__':
    main()