from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, LongType
import datetime
import json
import boto3
import os

s3r = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                     aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN)

bucket = s3r.Bucket('meetupprojectbucket')


def upload_to_s3(s3_path, json_result):
    if not os.path.exists(os.path.dirname(s3_path)):
        os.makedirs(os.path.dirname(s3_path))

    with open(s3_path, 'w', encoding='utf-8') as f:
        json.dump(json_result, f, ensure_ascii=False, indent=4)
    bucket.upload_file(s3_path, s3_path)


group_schema = [StructField("group_topics", ArrayType(StructType([StructField("topic_name", StringType())]))),
                StructField("group_country", StringType()),
                StructField("group_name", StringType()),
                StructField("group_state", StringType())]

whole_schema = StructType([
    StructField("mtime", LongType(), nullable=False),
    StructField("event", StructType([StructField("event_name", StringType())])),
    StructField("group", StructType(group_schema))
])

conf = SparkConf().setAppName("Learning_Spark").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

events = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .load()


# print(events.value.take(1))
strip = F.udf(lambda string: string.decode("utf-8").replace("\\", "")[1:-1], StringType())
get_time = F.udf(lambda ms: str(datetime.datetime.fromtimestamp(ms / 1000.0)), StringType())
get_hour = F.udf(lambda ms: datetime.datetime.fromtimestamp(ms / 1000.0).hour, IntegerType())
hour = datetime.datetime.now().hour

df = events.withColumn("value", strip(F.col("value"))) \
    .withColumn("value", F.from_json(F.col("value"), whole_schema)) \
    .select("value.*") \
    .filter(F.col("mtime").isNotNull()) \
    .withColumn("time", get_time(F.col("mtime"))) \
    .withColumn("hour", get_hour(F.col("mtime"))) \
    .where(((hour - 8 < F.col("hour")) & (F.col("hour") < (hour - 1))) | \
           ((hour + 16 < F.col("hour")) & (F.col("hour") < (hour + 23))))


# First query
countries = df.select("group.group_country") \
    .groupBy("group_country").count()
# convert DataFrame to json
json_result_str = countries.toJSON().collect()
json_result = {"time_start": f"{(hour - 7)%24}:00", "time_end": f"{(hour - 1)%24}:00", "statistics": []}
for line in json_result_str:
    line = json.loads(line)
    json_result["statistics"].append({line["group_country"]: line["count"]})
upload_to_s3(f"api1/q1_{hour}.json", json_result)
# print(json_result)


# Second query
states_groups = df.where(((hour - 5 < F.col("hour")) & (F.col("hour") < hour - 1)) | \
                         ((hour + 19 < F.col("hour")) & (F.col("hour") < (hour + 23)))) \
    .filter(F.col("group.group_state").isNotNull()) \
    .groupBy("group.group_state") \
    .agg(F.collect_list("group.group_name").alias("group_names"))
# convert DataFrame to json
json_result_str = states_groups.toJSON().collect()
json_result = {"time_start": f"{(hour - 4)%24}:00", "time_end": f"{(hour - 1)%24}:00", "statistics": []}
for line in json_result_str:
    line = json.loads(line)
    json_result["statistics"].append({line["group_state"]: line["group_names"]})
upload_to_s3(f"api1/q2_{hour}.json", json_result)
# print(json_result)


# Third query
window = Window.partitionBy("group_country").orderBy(F.desc("count"))
topics = df.withColumn("group_topics", F.udf(lambda x: list(zip(*x))[0], ArrayType(StringType()))("group.group_topics")) \
    .withColumn("group_topic", F.explode("group_topics")) \
    .groupby(["group.group_country", "group_topic"]).count() \
    .withColumn('order', F.row_number().over(window)) \
    .where(F.col('order') == 1)
# convert DataFrame to json
json_result_str = topics.toJSON().collect()
json_result = {"time_start": f"{(hour - 7)%24}:00", "time_end": f"{(hour - 1)%24}:00", "statistics": []}
for line in json_result_str:
    line = json.loads(line)
    json_result["statistics"].append({line["group_country"]: {line["group_topic"]: line["count"]}})
upload_to_s3(f"api1/q3_{hour}.json", json_result)
