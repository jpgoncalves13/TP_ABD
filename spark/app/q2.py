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
        print(f'{f.__name__} avg time over {NUMBER_OF_RUNS} runs: {round(avg_time, 3)}s')
        return result
    return wrap

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')

def count_rows(iterator):
    yield len(list(iterator))

@timeit
def q2(users, votes_with_types, max_reputation_per_year, creationDate=None, bucketInterval=None, spark=None):
    answers = spark.read.parquet(f"/app/fullDataParquet/answers.parquet")
    
    creationDate = creationDate if creationDate is not None else (F.now() - F.expr("INTERVAL 5 YEARS"))
    bucketInterval = bucketInterval or 5000
    
    buckets = max_reputation_per_year.withColumn("reputation_range", F.explode(F.sequence( F.lit(0), F.col("reputation_range"), F.lit(bucketInterval))))
    
    postids = votes_with_types.where(votes_with_types.CreationDate >= creationDate).select("PostId").distinct()
                
    answersids = answers.join(postids, answers.Id == postids.PostId).select("OwnerUserId").distinct()                    
    users_filtered = users.join(answersids, users.Id == answersids.OwnerUserId).distinct()
    
    return buckets \
        .join(users_filtered, (users_filtered.UserYear == buckets.year) & ((F.floor(users_filtered.Reputation / bucketInterval) * bucketInterval) == buckets.reputation_range), "left") \
        .groupBy("year", "reputation_range") \
        .agg(F.count("Id").alias("total")) \
        .orderBy("year", "reputation_range")
                                    
def main():
        
    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        
        # Options to opti mize spark
        #.config("spark.sql.adaptive.enabled", "false") \
        #.config("spark.executor.memory", "1g") \
            
    users = spark.read.parquet(f"/app/fullDataParquet/users.parquet").select("Id", "CreationDate", "Reputation").withColumn("UserYear", F.year("CreationDate")).cache() 
    votes_types = spark.read.parquet(f"/app/fullDataParquet/votes_types.parquet")  
    votes = spark.read.parquet(f"/app/fullDataParquet/votes.parquet")
    
    max_reputation_per_year = users.withColumn("year", F.year("CreationDate")).groupBy("year").agg(F.max("Reputation").cast("int").alias("reputation_range"))
    max_reputation_per_year.cache()
    votes_with_types = votes.join(votes_types, votes_types.Id == votes.VoteTypeId).filter(votes_types.Name == "AcceptedByOriginator")
    votes_with_types.cache()
   
    q2(users, votes_with_types, max_reputation_per_year,F.now() - F.expr("INTERVAL 6 YEARS"), 6000, spark=spark).show()    
    
    users.unpersist()
    max_reputation_per_year.unpersist()
    votes_with_types.unpersist()
    

if __name__ == '__main__':
    main()
    