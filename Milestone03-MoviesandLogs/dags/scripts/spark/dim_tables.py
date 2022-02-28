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


def dim_devices_table(input_loc, output_loc):
    """
   description
    """

    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    dim_devices  = log_reviews.dropDuplicates((['device'])).select(monotonically_increasing_id().alias('monotonically_increasing_id'), 'device')
    window = Window.orderBy(col('monotonically_increasing_id'))
    dim_devices = dim_devices.withColumn('id_dim_devices', row_number().over(window)).select('id_dim_devices','device')

    # parquet is a popular column storage format, we use it here
    #df_out.write.mode("overwrite").parquet(output_loc)
    #df_out.write.format("csv").mode("overwrite").save("s3://staging-raquel/clean_data/log_review.csv")
    dim_devices.write.csv(output_loc, mode='overwrite',header=True)

def dim_os_table(input_loc, output_loc):
 
    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    dim_os  = log_reviews.dropDuplicates((['os'])).select(monotonically_increasing_id().alias('monotonically_increasing_id'), 'os')
    window = Window.orderBy(col('monotonically_increasing_id'))
    dim_os  = dim_os.withColumn('id_dim_os', row_number().over(window)).select('id_dim_os','os')

    dim_os.write.csv(output_loc, mode='overwrite',header=True)

def dim_location_table(input_loc, output_loc):
     
    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    dim_location  = log_reviews.dropDuplicates((['location'])).select(monotonically_increasing_id().alias('monotonically_increasing_id'), 'location')
    window = Window.orderBy(col('monotonically_increasing_id'))
    dim_location  = dim_location.withColumn('id_dim_location', row_number().over(window)).select('id_dim_location','location')
    
    dim_location.write.csv(output_loc, mode='overwrite',header=True)

def dim_browser_table(input_loc, output_loc):
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    log_reviews = log_reviews.withColumn("browser", when(log_reviews.os == 'Microsoft Windows', "Internet Explorer")
                                 .when(log_reviews.os == 'Microsoft Windows', "Internet Explorer")
                                 .when(log_reviews.os.isin ("Apple iOS","Apple MacOS"), "Safari")
                                 .when(log_reviews.os == "Google Android", "Google Chrome")
                                 .when(log_reviews.os == "Linux", "Firefox")
                                 .otherwise('NULL'))
                                 
    dim_browser  = log_reviews.dropDuplicates((['browser'])).select(monotonically_increasing_id().alias('monotonically_increasing_id'), 'browser')
    window = Window.orderBy(col('monotonically_increasing_id'))
    dim_browser  = dim_browser.withColumn('id_dim_browser', row_number().over(window)).select('id_dim_browser','browser')

    dim_browser.write.csv(output_loc, mode='overwrite',header=True)


def dim_date_table(input_loc, output_loc):
     
    # read input 
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    dim_date  = log_reviews.dropDuplicates((['log_date'])).select(monotonically_increasing_id().alias('monotonically_increasing_id'), 'log_date')
    window = Window.orderBy(col('monotonically_increasing_id'))
    dim_date  = dim_date.withColumn('id_dim_date', row_number().over(window)).select('id_dim_date','log_date')
    dim_date= dim_date.withColumn('format', to_date(col("log_date"),"MM-dd-yyyy")).select('id_dim_date', 'log_date', dayofmonth("format").alias('day'), month("format").alias('month'), year("format").alias('year'))
    
    dim_date = dim_date.withColumn("season", when(dim_date.month.isin(12, 1, 2), "Winter")\
                                 .when(dim_date.month.isin(4, 5, 3), "Spring")\
                                 .when(dim_date.month.isin(6, 7, 8), "Summer")\
                                 .otherwise("Fall"))
    
    dim_date.write.csv(output_loc, mode='overwrite',header=True)



if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--input", type=str, help="HDFS input", default="/movie")
    #parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    #args = parser.parse_args()
    #spark = spark.sparkContext.setLogLevel("WARN")
    input_user_purchase = "s3://staging-raquel/user_purchase.csv"
    input_log = "s3://staging-raquel/clean_data/log_review/log_review.csv/"
    input_movies = "s3://staging-raquel/clean_data/movies_review/movies_review.csv/"
    output_devices = "s3://staging-raquel/tables/dim_tables/dim_devices.csv/"
    output_os = "s3://staging-raquel/tables/dim_tables/dim_os.csv/"
    output_location = "s3://staging-raquel/tables/dim_tables/dim_location.csv/"
    output_browser = "s3://staging-raquel/tables/dim_tables/dim_browser.csv/"
    output_date = "s3://staging-raquel/tables/dim_tables/dim_date.csv/"
    output_fact = "s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/"
    spark = SparkSession.builder.appName("Dim tables creation").getOrCreate()
    dim_devices_table(input_loc=input_log, output_loc=output_devices)
    dim_os_table(input_loc=input_log, output_loc=output_os)
    dim_location_table(input_loc=input_log, output_loc=output_location)
    dim_browser_table(input_loc=input_log, output_loc=output_browser)
    dim_date_table(input_loc=input_log, output_loc=output_date)

 
    
   