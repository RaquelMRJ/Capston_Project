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
from pyspark.sql.functions import sum

def fact_movie_table(input_user_purchase, input_log,input_movies, input_date, input_location, input_browser, input_device, input_os, output_fact):
     

    #schema = StructType([\
    #StructField("CustomerID", IntegerType(), True),\
    #StructField("id_review", IntegerType(), True),\
    #StructField("positive_review", StringType(), True)])

    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_log)
    log_reviews = log_reviews.withColumn("browser", when(log_reviews.os == 'Microsoft Windows', "Internet Explorer")
                                 .when(log_reviews.os == 'Microsoft Windows', "Internet Explorer")
                                 .when(log_reviews.os.isin ("Apple iOS","Apple MacOS"), "Safari")
                                 .when(log_reviews.os == "Google Android", "Google Chrome")
                                 .when(log_reviews.os == "Linux", "Firefox")
                                 .otherwise('NULL'))

    movie_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_movies)
    #movie_reviews = spark.read.option("header", True).schema(schema).csv(input_movies)
    user_purchase = spark.read.option("header", True).option("inferSchema", "true").csv(input_user_purchase)

    #Read dim tables
    dim_date = spark.read.option("header", True).option("inferSchema", "true").csv(input_date)
    dim_location = spark.read.option("header", True).option("inferSchema", "true").csv(input_location)
    dim_browser = spark.read.option("header", True).option("inferSchema", "true").csv(input_browser)
    dim_devices = spark.read.option("header", True).option("inferSchema", "true").csv(input_device)
    dim_os = spark.read.option("header", True).option("inferSchema", "true").csv(input_os)
    
    log_reviews.createOrReplaceTempView("log_reviews")
    user_purchase.createOrReplaceTempView("user_purchase")
    movie_reviews.createOrReplaceTempView("movie_reviews")

    dim_date.createOrReplaceTempView("DIM_DATE")
    dim_location.createOrReplaceTempView("DIM_LOCATION")
    dim_browser.createOrReplaceTempView("DIM_BROWSER")
    dim_devices.createOrReplaceTempView("DIM_DEVICES")
    dim_os.createOrReplaceTempView("DIM_OS")

    fact_table = spark.sql("SELECT \
                       S1.ID_DIM_DATE, \
                       S1.ID_DIM_LOCATION,\
                       S1.ID_DIM_DEVICES, \
                       S1.ID_DIM_OS,\
                       S1.ID_DIM_BROWSER, \
                       SUM(S2.AMOUNT_SPENT) AS AMOUNT_SPENT, \
                       SUM(S1.REVIEW_SCORE) AS REVIEW_SCORE, \
                       SUM(S1.REVIEW_COUNT) AS REVIEW_COUNT, \
                       current_timestamp () AS INSERT_DATE \
                   FROM \
                   (SELECT \
                       LOGS.ID_REVIEW,\
                       DIM_DATE.ID_DIM_DATE,\
                       DIM_LOCATION.ID_DIM_LOCATION,\
                       DIM_DEVICES.ID_DIM_DEVICES, \
                       DIM_OS.ID_DIM_OS, \
                       DIM_BROWSER.ID_DIM_BROWSER,\
                       SUM(REVIEW.POSITIVE_REVIEW) AS REVIEW_SCORE,\
                       COUNT(REVIEW.id_review) AS REVIEW_COUNT \
                    FROM log_reviews AS LOGS \
                       INNER JOIN movie_reviews AS REVIEW ON LOGS.ID_REVIEW = REVIEW.id_review \
                       LEFT JOIN DIM_DATE AS DIM_DATE ON LOGS.LOG_DATE = DIM_DATE.LOG_DATE \
                       LEFT JOIN DIM_LOCATION AS DIM_LOCATION ON LOGS.location = DIM_LOCATION.location \
                       LEFT JOIN DIM_DEVICES AS DIM_DEVICES ON LOGS.DEVICE = DIM_DEVICES.DEVICE \
                       LEFT JOIN DIM_OS AS DIM_OS ON LOGS.OS = DIM_OS.OS \
                       LEFT JOIN DIM_BROWSER AS DIM_BROWSER ON LOGS.BROWSER = DIM_BROWSER.BROWSER \
                   GROUP BY \
                       DIM_DATE.ID_DIM_DATE, \
                       DIM_LOCATION.ID_DIM_LOCATION,\
                       DIM_DEVICES.ID_DIM_DEVICES, \
                       DIM_OS.ID_DIM_OS, \
                       DIM_BROWSER.ID_DIM_BROWSER, \
                       LOGS.ID_REVIEW) AS S1 \
                       LEFT JOIN ( \
                           SELECT \
                              s2LOGS.ID_REVIEW, \
                              SUM(s2UP.QUANTITY * s2UP.UnitPrice) AS AMOUNT_SPENT \
                           FROM \
                              LOG_REVIEWS AS s2LOGS \
                              INNER JOIN movie_reviews AS s2REVIEW ON s2LOGS.ID_REVIEW = s2REVIEW.id_review \
                              INNER JOIN USER_PURCHASE AS s2UP ON s2REVIEW.user_id = s2UP.CUSTOMERID \
                              LEFT JOIN DIM_DATE AS s2DIM_DATE ON s2LOGS.LOG_DATE = s2DIM_DATE.LOG_DATE \
                           GROUP BY \
                              s2LOGS.ID_REVIEW) AS S2 ON S1.ID_REVIEW = S2.ID_REVIEW \
                   GROUP BY \
                       S1.ID_DIM_DATE, \
                       S1.ID_DIM_LOCATION, \
                       S1.ID_DIM_DEVICES, \
                       S1.ID_DIM_OS, \
                       S1.ID_DIM_BROWSER") 

    fact_table = fact_table.select(monotonically_increasing_id().alias('monotonically_increasing_id'), '*')
    window = Window.orderBy(col('monotonically_increasing_id'))
    fact_table  = fact_table.withColumn('id_fact_movie_analytics', row_number().over(window)).select('id_fact_movie_analytics', 'ID_DIM_DATE', 'ID_DIM_LOCATION', 'ID_DIM_DEVICES', 'ID_DIM_OS', 'ID_DIM_BROWSER', 'AMOUNT_SPENT', 'REVIEW_SCORE', 'REVIEW_COUNT', 'INSERT_DATE')
    
    fact_table.write.csv(output_fact, mode='overwrite',header=True)


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
    input_location = "s3://staging-raquel/tables/dim_tables/dim_location.csv/"
    input_browser = "s3://staging-raquel/tables/dim_tables/dim_browser.csv/"
    input_device = "s3://staging-raquel/tables/dim_tables/dim_devices.csv/"
    input_os = "s3://staging-raquel/tables/dim_tables/dim_os.csv/"
    output_fact = "s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/"
    spark = SparkSession.builder.appName("Fact Table creation").getOrCreate()
    fact_movie_table(input_user_purchase, input_log,input_movies, input_date, input_location, input_browser, input_device, input_os, output_fact)

    
   