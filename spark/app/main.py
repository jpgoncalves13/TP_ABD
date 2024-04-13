
from typing import List
from pyspark.sql import SparkSession, DataFrame, Row, Column
import pyspark.sql.functions as F 
from datetime import datetime
from functools import wraps
from pprint import pprint
import time
import sys

# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        print(f'{f.__name__}: {round(time.time() - t, 3)}s')
        return result
    return wrap

def count_rows(iterator):
    yield len(list(iterator))

# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    for partition, rows in enumerate(df.rdd.mapPartitions(count_rows).collect()):
        print(f'Partition {partition} has {rows} rows')

@timeit
def q1(users: DataFrame, questions: DataFrame, answers: DataFrame, comments: DataFrame, creationDate: Column=None) -> List[Row]:
    creationDate = creationDate or (F.now() - F.expr("INTERVAL 6 MONTHS"))
    
    recent_questions = questions.filter((questions.CreationDate >= creationDate) & (questions.CreationDate <= F.now()))
    recent_answers = answers.filter((answers.CreationDate >= creationDate) & (answers.CreationDate <= F.now()))
    recent_comments = comments.filter((comments.CreationDate >= creationDate) & (comments.CreationDate <= F.now()))

    return users \
        .join(recent_questions, users.Id == recent_questions.OwnerUserId, "left").select(users["*"], recent_questions.Id.alias("q_id")) \
        .join(recent_answers, users.Id == recent_answers.OwnerUserId, "left").select(users["*"], "q_id", recent_answers.Id.alias("a_id")) \
        .join(recent_comments, users.Id == recent_comments.UserId, "left").select(users["*"], "q_id", "a_id", recent_comments.Id.alias("c_id")) \
        .groupBy("Id", "DisplayName").agg((F.countDistinct("q_id") + F.countDistinct("a_id") + F.countDistinct("c_id")).alias("total")) \
        .orderBy("total", ascending=False).limit(100).collect()

@timeit
def q1_sql(spark: SparkSession):
    query = """
    SELECT id, displayname,
        count(DISTINCT q_id) + count(DISTINCT a_id) + count(DISTINCT c_id) total
    FROM (
        SELECT u.*, q.id q_id, a.id a_id, c.id c_id
        FROM users u
        LEFT JOIN (
            SELECT *
            FROM questions
            WHERE creationdate BETWEEN now() - interval '6 months' AND now()
        ) q ON q.owneruserid = u.id
        LEFT JOIN (
            SELECT *
            FROM answers
            WHERE creationdate BETWEEN now() - interval '6 months' AND now()
        ) a ON a.owneruserid = u.id
        LEFT JOIN (
            SELECT *
            FROM comments
            WHERE creationdate BETWEEN now() - interval '6 months' AND now()
        ) c ON c.userid = u.id
    ) t
    GROUP BY id, displayname
    ORDER BY total DESC
    LIMIT 100;
    """
    return spark.sql(query).collect()

@timeit
def q2(votes: DataFrame, votes_types: DataFrame, users: DataFrame, answers: DataFrame, creationDate: Column=None, bucketInterval:int =None) -> List[Row]:
    creationDate = creationDate or (F.now() - F.expr("INTERVAL 5 YEARS"))
    bucketInterval = bucketInterval or 5000
    
    buckets = users.withColumn("year", F.year("CreationDate")) \
                    .groupBy("year").agg(F.max("Reputation").cast("int").alias("reputation_range")) \
                    .withColumn("reputation_range", F.explode(F.sequence( F.lit(0), F.col("reputation_range"), F.lit(bucketInterval))))
    
    postids = votes.join(votes_types, votes_types.Id == votes.VoteTypeId) \
                .where((votes_types.Name == "AcceptedByOriginator") & (votes.CreationDate >= creationDate)).select("PostId").distinct()
                
    answersids = answers.join(postids, answers.Id == postids.PostId).select("OwnerUserId").distinct()                    
    users_filtered = users.join(answersids, users.Id == answersids.OwnerUserId).select("Id", "CreationDate", "Reputation").distinct()
    
    return buckets \
        .join(users_filtered, (F.year(users_filtered.CreationDate) == buckets.year) & ((F.floor(users_filtered.Reputation / 5000) * 5000) == buckets.reputation_range), "left") \
        .groupBy("year", "reputation_range") \
        .agg(F.count("Id").alias("total")) \
        .orderBy("year", "reputation_range").collect()
    
@timeit
def q3(tags: DataFrame, questions_tags: DataFrame, questions: DataFrame, answers: DataFrame, inferior_count_limit: int=None) -> List[Row]:
    inferior_count_limit = inferior_count_limit or 10
    return
        
        
@timeit 
def q3_sql(spark: SparkSession):
    query = """
    SELECT tagname, round(avg(total), 3), count(*)
    FROM (
        SELECT t.tagname, q.id, count(*) AS total
        FROM tags t
        JOIN questionstags qt ON qt.tagid = t.id
        JOIN questions q ON q.id = qt.questionid
        LEFT JOIN answers a ON a.parentid = q.id
        WHERE t.id IN (
            SELECT t.id
            FROM tags t
            JOIN questionstags qt ON qt.tagid = t.id
            JOIN questions q ON q.id = qt.questionid
            GROUP BY t.id
            HAVING count(*) > 10
        )
        GROUP BY t.tagname, q.id
    )
    GROUP BY tagname
    ORDER BY 2 DESC, 3 DESC, tagname;
    """
    return spark.sql(query).collect()

@timeit
def q4(badges: DataFrame, bucket_size: int=None) -> List[Row]:
    bucket_size = bucket_size or 1

    return

@timeit
def q4_sql(spark: SparkSession):
    query = """
    SELECT date_bin('1 minute', date, '2008-01-01 00:00:00'), count(*)
    FROM badges
    WHERE NOT tagbased
        AND name NOT IN (
            'Analytical',
            'Census',
            'Documentation Beta',
            'Documentation Pioneer',
            'Documentation User',
            'Reversal',
            'Tumbleweed'
        )
        AND class in (1, 2, 3)
        AND userid <> -1
    GROUP BY 1
    ORDER BY 1;
    """ 
    return spark.sql(query).collect()

def main():
    @timeit
    def w1():
        q1(users, questions, answers, comments)

    @timeit
    def w2():
        q2(votes, votes_types, users, answers)
        
    @timeit
    def w3():
        tags.createOrReplaceTempView("tags")
        questions_tags.createOrReplaceTempView("questionstags")
        questions.createOrReplaceTempView("questions")
        answers.createOrReplaceTempView("answers")
        
    @timeit
    def w4():
        badges.createOrReplaceTempView("badges")

    if len(sys.argv) < 2:
        print('Missing function name. Usage: python3 main.py <function-name>')
        return
    elif sys.argv[1] not in locals():
        print(f'No such function: {sys.argv[1]}')
        return

    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        
        # Options to optimize spark
        #.config("spark.sql.adaptive.enabled", "false") \
        #.config("spark.executor.memory", "1g") \
        
        
        # For google cloud
        # .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.21.jar") \
        # .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.21.jar") \
    
    # For google cloud
    # google cloud service account credentials file
    # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/app/credentials.json")
    # BUCKET_NAME = 'tp_abd'
    # answers = spark.read.parquet(f"gs://{BUCKET_NAME}/answers")

    # data frames
    answers = spark.read.csv("/app/data/Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    badges = spark.read.csv("/app/data/Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    comments = spark.read.csv("/app/data/Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    questions = spark.read.csv("/app/data/Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    questions_links = spark.read.csv("/app/data/QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    questions_tags = spark.read.csv("/app/data/QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    tags = spark.read.csv("/app/data/Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    users = spark.read.csv("/app/data/Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    votes = spark.read.csv("/app/data/Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    votes_types = spark.read.csv("/app/data/VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')

    locals()[sys.argv[1]]()


if __name__ == '__main__':
    main()