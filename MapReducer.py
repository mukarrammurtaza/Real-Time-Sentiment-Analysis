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
# Define HDFS input, output, and checkpoint paths
hdfs_input_path = "/SentimentAnalysis/raw_data"
hdfs_output_path = f"/SentimentAnalysis/MapReduceResults_{timestamp}/"
checkpoint_path = f"/SentimentAnalysis/checkpoint_{timestamp}/"



mapreduce_command = f"/usr/local/hadoop/bin/hadoop jar /home/mukarram/MR-SA/mapreduce.jar org.myorg.MrManager {hdfs_input_path} {hdfs_output_path} -skip /SentimentAnalysis/stop-words.txt -pos /SentimentAnalysis/positive_words.txt -neg /SentimentAnalysis/negative_words.txt"

# Trigger MapReduce job periodically
while True:
    try:
        subprocess.run(mapreduce_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running MapReduce job: {e}")
    time.sleep(60 * 30)  # Sleep for 30 minutes before triggering again