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
def q3(inferior_count_limit: int = 10, spark=None):

    tags = spark.read.parquet(f"/app/fullDataParquet/tags.parquet")
    questions_tags = spark.read.parquet(f"/app/fullDataParquet/questions_tags.parquet")
    questions = spark.read.parquet(f"/app/fullDataParquet/questions.parquet")
    answers = spark.read.parquet(f"/app/fullDataParquet/answers.parquet")
    
    # Inferior count limit
    inferior_count_limit = inferior_count_limit or 10

    # Subquery to find tag ids with more than `inferior_count_limit` questions
    question_counts = questions_tags.groupby('tagid').count()
    filtered_tags = question_counts.filter(question_counts['count'] > inferior_count_limit).select('tagid')

    # Filter tags
    filtered_tags_df = tags.join(filtered_tags, tags['id'] == filtered_tags['tagid'])

    # Use alias to avoid ambiguity
    tags_questions = filtered_tags_df.alias('t').join(questions_tags.alias('qt'), F.col('t.id') == F.col('qt.tagid'))
    tagged_questions = tags_questions.alias('tq').join(questions.alias('q'), F.col('tq.questionid') == F.col('q.id'))

    # Join questions with answers and count answers per question using alias
    tagged_questions_with_answers = tagged_questions.alias('tq').join(answers.alias('a'), F.col('tq.questionid') == F.col('a.parentid'), 'left')
    answer_counts = tagged_questions_with_answers.groupby('tq.tagname', 'tq.questionid').count()

    # Calculate average and count per tag
    result = answer_counts.groupby('tagname').agg(
        F.round(F.avg('count'), 3).alias('avg_total'),
        F.count('count').alias('count')
    )

    # Order by average total descending, count descending, and tagname
    result = result.orderBy(F.desc('avg_total'), F.desc('count'), 'tagname')

    return result

@timeit
def q3_csv(tags: DataFrame, questions_tags: DataFrame, questions: DataFrame, answers: DataFrame, inferior_count_limit: int = 10) -> DataFrame:
    # Inferior count limit
    inferior_count_limit = inferior_count_limit or 10

    # Subquery to find tag ids with more than `inferior_count_limit` questions
    question_counts = questions_tags.groupby('tagid').count()
    filtered_tags = question_counts.filter(question_counts['count'] > inferior_count_limit).select('tagid')

    # Filter tags
    filtered_tags_df = tags.join(filtered_tags, tags['id'] == filtered_tags['tagid'])

    # Use alias to avoid ambiguity
    tags_questions = filtered_tags_df.alias('t').join(questions_tags.alias('qt'), F.col('t.id') == F.col('qt.tagid'))
    tagged_questions = tags_questions.alias('tq').join(questions.alias('q'), F.col('tq.questionid') == F.col('q.id'))

    # Join questions with answers and count answers per question using alias
    tagged_questions_with_answers = tagged_questions.alias('tq').join(answers.alias('a'), F.col('tq.questionid') == F.col('a.parentid'), 'left')
    answer_counts = tagged_questions_with_answers.groupby('tq.tagname', 'tq.questionid').count()

    # Calculate average and count per tag
    result = answer_counts.groupby('tagname').agg(
        F.round(F.avg('count'), 3).alias('avg_total'),
        F.count('count').alias('count')
    )

    # Order by average total descending, count descending, and tagname
    result = result.orderBy(F.desc('avg_total'), F.desc('count'), 'tagname')

    return result.collect()

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
    return spark.sql(query).show()

def main():
        
    # the spark session
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()
        
        # .config("spark.sql.shuffle.partitions", "12") \

    #answers = spark.read.parquet("/app/fullDataParquet/answers.parquet")
    #badges = spark.read.parquet("/app/fullDataParquet/badges.parquet")
    #comments = spark.read.parquet("/app/fullDataParquet/comments.parquet")
    #questions = spark.read.parquet("/app/fullDataParquet/questions.parquet")
    #questions_links = spark.read.parquet("/app/fullDataParquet/questions_links.parquet")
    #questions_tags = spark.read.parquet("/app/fullDataParquet/questions_tags.parquet")
    #tags = spark.read.parquet("/app/fullDataParquet/tags.parquet")
    #users = spark.read.parquet("/app/fullDataParquet/users.parquet")
    #votes = spark.read.parquet("/app/fullDataParquet/votes.parquet")
    #votes_types = spark.read.parquet("/app/fullDataParquet/votes_types.parquet")

    #answers = spark.read.csv("/app/fullData/Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #badges = spark.read.csv("/app/fullData/Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #comments = spark.read.csv("/app/fullData/Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #questions = spark.read.csv("/app/fullData/Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #questions_links = spark.read.csv("/app/fullData/QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #questions_tags = spark.read.csv("/app/fullData/QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #tags = spark.read.csv("/app/fullData/Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #users = spark.read.csv("/app/fullData/Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #votes = spark.read.csv("/app/fullData/Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
    #votes_types = spark.read.csv("/app/fullData/VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')

    #tags.createOrReplaceTempView("tags")
    #questions_tags.createOrReplaceTempView("questionstags")
    #questions.createOrReplaceTempView("questions")
    #answers.createOrReplaceTempView("answers")
    
    q3(spark=spark).show()


if __name__ == '__main__':
    main()