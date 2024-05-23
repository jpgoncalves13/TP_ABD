from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .getOrCreate()

answers = spark.read.csv("/app/fullData/Answers.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
badges = spark.read.csv("/app/fullData/Badges.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
comments = spark.read.csv("/app/fullData/Comments.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questions = spark.read.csv("/app/fullData/Questions.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questions_links = spark.read.csv("/app/fullData/QuestionsLinks.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
questions_tags = spark.read.csv("/app/fullData/QuestionsTags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
tags = spark.read.csv("/app/fullData/Tags.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
users = spark.read.csv("/app/fullData/Users.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votes = spark.read.csv("/app/fullData/Votes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')
votes_types = spark.read.csv("/app/fullData/VotesTypes.csv", header=True, inferSchema=True, multiLine=True, escape='\"')


answers.write.parquet("/app/fullDataParquet/answers.parquet", mode="overwrite")
badges.write.parquet("/app/fullDataParquet/badges.parquet", mode="overwrite") 
comments.write.parquet("/app/fullDataParquet/comments.parquet", mode="overwrite") 
questions.write.parquet("/app/fullDataParquet/questions.parquet", mode="overwrite") 
questions_links.write.parquet("/app/fullDataParquet/questions_links.parquet", mode="overwrite") 
questions_tags.write.parquet("/app/fullDataParquet/questions_tags.parquet", mode="overwrite") 
tags.write.parquet("/app/fullDataParquet/tags.parquet", mode="overwrite") 
users.write.parquet("/app/fullDataParquet/users.parquet", mode="overwrite") 
votes.write.parquet("/app/fullDataParquet/votes.parquet", mode="overwrite") 
votes_types.write.parquet("/app/fullDataParquet/votes_types.parquet", mode="overwrite") 

spark.stop()