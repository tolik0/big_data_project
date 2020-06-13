from pyspark.sql.types import StringType,StructType, StructField, IntegerType, FloatType, TimestampType, ArrayType, LongType
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
import json
import pyspark

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

CASSANDRA_KEYSPACE = "test3"


event_schema = [StructField("event_name", StringType()),
                StructField("time", LongType()),
                StructField('event_id', StringType())]

topics_schema = [StructField("topic_name", StringType())]

group_schema = [StructField("group_topics", ArrayType(StructType(topics_schema))),
                StructField("group_city", StringType()),
                StructField("group_country", StringType()),
                StructField("group_id", LongType()),
                StructField("group_name", StringType()),
                StructField("group_state", StringType())]

whole_schema = StructType([StructField("mtime", LongType()),
                           StructField("event", StructType(event_schema)),
                           StructField("group", StructType(group_schema))])


def write_df2cassandra(df, table, keyspace=CASSANDRA_KEYSPACE):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table=table, keyspace=keyspace) \
        .save()



def procces(rdd):
    if rdd.count() < 1:
        return



    df = sqlContext.createDataFrame(data=rdd, schema=StringType())

    df = df.withColumn("value", F.from_json(F.col("value"), whole_schema)) \
        .select("value.*")


    # for 1 and 2 query
    cities = df.select(F.col("group.group_country"), F.col("group.group_city")).filter(F.col("group.group_city").isNotNull()).distinct()


    # # for 5 query
    events = df.select(F.col("event.event_name"), F.col("event.event_id").cast(LongType()), F.col("event.time"),
                       F.col("group.group_topics"), F.col("group.group_name"), F.col("group.group_country"),
                       F.col("group.group_city"), F.col("group.group_id").cast(LongType())) \
        .withColumn("group_topics",
                    F.udf(lambda x: list(zip(*x))[0] if x is not None else [], ArrayType(StringType()))("group_topics")).filter(F.col("event_id").isNotNull()).filter(F.col("group_id").isNotNull())



    # # for 3 query
    events_by_id = events.select(F.col("event_name"), F.col("event_id"), F.col("time"),
                                 F.col("group_name"), F.col("group_country"), F.col("group_city"),
                                 F.col("group_topics")).filter(F.col("event_id").isNotNull())
    

    # for 4 query
    groups = df.select(F.col("group.group_name"), F.col("group.group_city"), F.col("group.group_id").cast(LongType())).filter(F.col("group.group_city").isNotNull())



    write_df2cassandra(cities, "cities")
    write_df2cassandra(events_by_id, "event_by_event_id")
    write_df2cassandra(groups, "groups_by_city_and_group_id")
    write_df2cassandra(events, "event_by_group_id")



if __name__ == "__main__":
    sc = SparkContext(appName="KafkaProcessor")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 1)
    sqlContext = SQLContext(sc)

    kafkaStream = KafkaUtils.createStream(ssc, '127.0.0.1:2181', 'test-consumer-group', {"events7h": 1})
    parsed = kafkaStream.map(lambda string: string[1].replace("\\", "")[1:-1])

    parsed.foreachRDD(procces)

    ssc.start()  # Start the computation
    ssc.awaitTermination()











