# pyspark
import argparse
import csv
#import xmltodict
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import datetime
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.window import *
#from pyspark.sql.functions import sum

def fact_movie_table(input_user_purchase, input_log,input_movies, input_date, input_location, input_device, input_os, output_fact):
     

    #schema = StructType([\
    #StructField("CustomerID", IntegerType(), True),\
    #StructField("id_review", IntegerType(), True),\
    #StructField("positive_review", StringType(), True)])

    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_log)
    movie_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_movies)
    #movie_reviews = spark.read.option("header", True).schema(schema).csv(input_movies)
    user_purchase = spark.read.option("header", True).option("inferSchema", "true").csv(input_user_purchase)

    #Read dim tables
    dim_date = spark.read.option("header", True).option("inferSchema", "true").csv(input_date)
    dim_location = spark.read.option("header", True).option("inferSchema", "true").csv(input_location)
    dim_devices = spark.read.option("header", True).option("inferSchema", "true").csv(input_device)
    dim_os = spark.read.option("header", True).option("inferSchema", "true").csv(input_os)
    
    merged = user_purchase.join(movie_reviews, user_purchase.CustomerID == movie_reviews.user_id).distinct()
    merged2 = merged.join(log_reviews, 'id_review').distinct()

    fact= merged2.join(dim_date, "log_date", 'left')\
             .join(dim_location, "location", 'left')\
             .join(dim_devices, "device", 'left')\
             .join(dim_os, "os", 'left')\
             .withColumn('amount_spent1', (col('Quantity') * col('UnitPrice')))\
             .withColumn('insert_date', F.current_timestamp()) \
             .select('CustomerID', 'id_dim_date', 'id_dim_devices', 'id_dim_location', 'id_dim_os', 'positive_review','amount_spent1', 'insert_date', 'id_review')\
             .distinct()

    fact = fact.withColumn('positive_review', fact.positive_review.cast(IntegerType()))
    fact_table = fact.groupBy('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'insert_date', 'positive_review', 'id_review')\
                 .agg(sum('amount_spent1').alias('amount_spent'))\
                 .select('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'amount_spent', 'insert_date', 'positive_review', 'id_review' )
    fact_table2 = fact_table.groupBy('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'amount_spent', 'insert_date', 'id_review')\
                 .agg(sum('positive_review').alias('review_score'))\
                 .select('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'amount_spent', 'review_score', 'insert_date','id_review')

    fact_table3 = fact_table2.groupBy('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'amount_spent', 'review_score', 'insert_date')\
                         .agg(count('id_review').alias('review_count'))\
                         .select('CustomerID', 'id_dim_date','id_dim_devices', 'id_dim_location', 'id_dim_os', 'amount_spent', 'review_score', 'review_count', 'insert_date')



    fact_table3.write.csv(output_fact, mode='overwrite',header=True)


if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--input", type=str, help="HDFS input", default="/movie")
    #parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    #args = parser.parse_args()
    #spark = spark.sparkContext.setLogLevel("WARN")
    input_user_purchase = "s3://staging-raquel/user_purchase.csv"
    input_log = "s3://staging-raquel/clean_data/log_review/log_review.csv/"
    input_movies = "s3://staging-raquel/clean_data/movies_review/movies_review.csv/"
    input_date = "s3://staging-raquel/tables/dim_tables/dim_date.csv/"
    input_location = "s3://staging-raquel/tables/dim_tables/dim_location.csv//"
    input_device = "s3://staging-raquel/tables/dim_tables/dim_devices.csv/"
    input_os = "s3://staging-raquel/tables/dim_tables/dim_os.csv/"
    output_fact = "s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/"
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    fact_movie_table(input_user_purchase, input_log,input_movies, input_date, input_location, input_device, input_os, output_fact)

    
   