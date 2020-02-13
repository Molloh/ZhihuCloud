# -*- coding: utf-8 -*-
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.streaming import *
from pyspark.sql.functions import *

inputPath = "hdfs://172.19.240.199:9000/input"
schema = StructType([
    StructField('_id', StringType()),
    StructField('id', StringType()),
    StructField('urlToken', StringType()),
    StructField('name', StringType()),
    StructField('isOrg', BooleanType()),
    StructField('type', StringType()),
    StructField('url', StringType()),
    StructField('headline', StringType()),
    StructField('isActive', LongType()),
    StructField('description', StringType()),
    StructField('gender', IntegerType()),
    StructField('badge', ArrayType(StringType())),
    StructField('followerCount', LongType()),
    StructField('followingCount', LongType()),
    StructField('answerCount', LongType()),
    StructField('questionCount', LongType()),
    StructField('commercialQuestionCount', LongType()),
    StructField('articlesCount', LongType()),
    StructField('favoritedCount', LongType()),
    StructField('pinsCount', LongType()),
    StructField('logsCount', LongType()),
    StructField('voteupCount', LongType()),
    StructField('thankedCount', LongType()),
    StructField('hostedLiveCount', LongType()),
    StructField('followingColumnsCount', LongType()),
    StructField('followingTopicCount', LongType()),
    StructField('followingQuestionCount', LongType()),
    StructField('followingFavlistsCount', LongType()),
    StructField('voteToCount', LongType()),
    StructField('voteFromCount', LongType()),
    StructField('thankToCount', LongType()),
    StructField('thankFromCount', LongType()),
    StructField('business', StringType()),
    StructField('locations', ArrayType(StringType())),
    StructField('educations', ArrayType(
        StructType([
            StructField("school", StringType()),
            StructField("major", StringType())
        ])
    )),
])

spark = SparkSession \
    .builder.master("spark://172.19.240.199:7077") \
    .appName("ZhihuStreaming") \
    .getOrCreate()

usr_df = spark.readStream.schema(schema).json(inputPath)

school_df = usr_df.select(explode('educations.school').alias('school'))
school_count = school_df.groupBy(school_df.school).count()

business_df = usr_df.select('business')
business_count = business_df.groupBy(business_df.business).count()

def updateSchool(df, epoch_id):
    df.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri", "mongodb://172.19.240.199:20000/CloudComputing.schoolcount").mode("overwrite").save()
    df.coalesce(1).write.mode("overwrite").json("hdfs://172.19.240.199:9000/output/school.json")

query_school = (
    school_count
        .writeStream
        .outputMode("complete")
        .foreachBatch(updateSchool)
        .trigger(processingTime='3 second')
        .queryName("count_schools")
        .start()
)

def updateBusiness(df, epoch_id):
    df.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri", "mongodb://172.19.240.199:20000/CloudComputing.businesscount").mode("overwrite").save()
    df.coalesce(1).write.mode("overwrite").json("hdfs://172.19.240.199:9000/output/business.json")

query_business = (
    business_count
        .writeStream
        .outputMode("complete")
        .foreachBatch(updateBusiness)
        .trigger(processingTime='3 second')
        .queryName("count_businesses")
        .start()
)

query_school.awaitTermination()
query_business.awaitTermination()