import subprocess
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import nltk
from nltk.stem import PorterStemmer
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# NLTK and Stemming Initialization
nltk.download("punkt")
stemmer = PorterStemmer()

# Define Schema for Reddit Data
schema = StructType([
    StructField("title", StringType()),
    StructField("comments", ArrayType(StringType()))
])

# InfluxDB Configuration
influxdb_host = 'localhost'
influxdb_port = 8086
influxdb_token = 'CLBALSs4T6MYecQne1oT2evzqZO5365Qclnu9UPCWkVX6vSggllKrQjrE-d11LgcaDvqi0XY_VQC2tcm2uUZzQ=='
influxdb_org = 'test'
influxdb_bucket = 'sentimentanalysis'

# Load Positive and Negative Words
def load_words(file_path):
    with open(file_path, 'r') as file:
        return set(file.read().splitlines())

positive_words = load_words("positive_words.txt")
negative_words = load_words("negative_words.txt")

# Sentiment Analysis Function
def sentiment_analysis(text, positive_words, negative_words):
    positive_count = sum(1 for word in text.split() if word in positive_words)
    negative_count = sum(1 for word in text.split() if word in negative_words)
    return (positive_count - negative_count) / (positive_count + negative_count) if (positive_count + negative_count) > 0 else 0

# Stemming Function
def stem_text(text):
    words = nltk.word_tokenize(text)
    stemmed_words = [stemmer.stem(word) for word in words]
    return " ".join(stemmed_words)

# Register UDFs
sentiment_analysis_udf = udf(lambda text: sentiment_analysis(text, positive_words, negative_words), FloatType())
stem_udf = udf(stem_text, StringType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeRedditSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.conf.dir", "/usr/local/hadoop/etc/hadoop") \
    .getOrCreate()


# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-stream") \
    .load() \
    .select(col("value").cast("string").alias("json")) \
    .select(from_json(col("json"), schema).alias("data"))

# Process Data
flattened = df.selectExpr("data.title", "explode(data.comments) as comment")
stemmed_df = flattened.withColumn("stemmed_comment", stem_udf(col("comment")))
sentiment_scores = stemmed_df.withColumn("sentiment_score", sentiment_analysis_udf(col("stemmed_comment")))

# Write to InfluxDB
def write_to_influxdb(df, epoch_id):
    pandas_df = df.toPandas()
    client = InfluxDBClient(url=f"http://{influxdb_host}:{influxdb_port}", token=influxdb_token, org=influxdb_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for index, row in pandas_df.iterrows():
        point = Point("sentimentscore") \
            .tag("title", row["title"]) \
            .field("sentiment_score", float(row["sentiment_score"]))
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)
    client.close()


query_influxdb = sentiment_scores.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_influxdb) \
    .start()

hdfs_input_path = "/SentimentAnalysis/raw_data/"
hdfs_output_path = f"/SentimentAnalysis/MapReduceResults_{timestamp}/"
checkpoint_path = f"/SentimentAnalysis/checkpoint_{timestamp}/"
query_hdfs = sentiment_scores.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", hdfs_input_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query_influxdb.awaitTermination()
query_hdfs.awaitTermination()


# MapReduce command
