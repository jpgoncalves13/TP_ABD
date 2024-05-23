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
def q1(users, creationDate=None, spark=None):
    
    answers = spark.read.parquet(f"/app/fullDataParquet/answers.parquet")
    comments = spark.read.parquet(f"/app/fullDataParquet/comments.parquet")
    questions = spark.read.parquet(f"/app/fullDataParquet/questions.parquet")
    
    creationDate = creationDate if creationDate is not None else (F.now() - F.expr("INTERVAL 6 MONTHS"))
    currentDate = F.now()
    
    recent_questions = questions.filter((questions.CreationDate >= creationDate) & (questions.CreationDate <= currentDate)).select('OwnerUserId', 'Id').withColumnRenamed('Id', 'q_Id')
    recent_answers = answers.filter((answers.CreationDate >= creationDate) & (answers.CreationDate <= currentDate)).select('OwnerUserId', 'Id').withColumnRenamed('Id', 'a_Id')
    recent_comments = comments.filter((comments.CreationDate >= creationDate) & (comments.CreationDate <= currentDate)).select('UserId', 'Id').withColumnRenamed('Id', 'c_Id')
    
    return users \
        .join(recent_questions, users.Id == recent_questions.OwnerUserId, "left") \
        .join(recent_answers, users.Id == recent_answers.OwnerUserId, "left") \
        .join(recent_comments, users.Id == recent_comments.UserId, "left") \
        .groupBy("Id", "DisplayName").agg((F.countDistinct("q_id") + F.countDistinct("a_id") + F.countDistinct("c_id")).alias("total")) \
        .orderBy("total", ascending=False).limit(100)
                                
def main():
        
    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        
        # .config("spark.sql.shuffle.partitions", "12") \
            
        
    users = spark.read.parquet(f"/app/fullDataParquet/users.parquet").select("Id", "DisplayName").cache()
    q1(users, spark=spark).show()    
    users.unpersist()
    

if __name__ == '__main__':
    main()