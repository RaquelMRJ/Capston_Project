# pyspark
import argparse
import csv
import xmltodict
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.functions import *

def get_str_from_xml(xml_str: str, path: str):
        """
        Get a specific value nested in the XML structure
        the path is a point-separated string which points
        to the nested value. For example:

        <html>
            <header>
                <h1>Welcome</h1>
            </header>
        </html
        """
        if not path:
            return ""

        # transform the xml string to a python dict
        d = xmltodict.parse(xml_str)

        # iterate through the dict until we find our key
        # if any of the keys does not exist, this will
        # generate an empty dict
    
        # d = {'reviewlog': 'x', 'path': 'y'}
        # path = "reviewwwwwlogggg"
    
        cursor = d
        for segment in path.split("."):
            cursor = cursor.get(segment, {})

        return cursor if cursor and isinstance(cursor, str) else ""

def movies_classifier(input_loc, output_loc):
    """
    description
    """

    # read input
    df_raw = spark.read.option("header", True).csv(input_loc)
    # perform text cleaning

    # Tokenize text
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    df_tokens = tokenizer.transform(df_raw).select("cid", "review_token", "id_review")

    # Remove stop words
    remover = StopWordsRemover(inputCol="review_token", outputCol="review_clean")
    #df_clean = remover.transform(df_tokens).select("cid", "review_clean", "id_review")
    df_clean = remover.transform(df_tokens).select("cid", "review_clean", "id_review")


    # function to check presence of good
    df_out = df_clean.withColumn("positive_review", when(array_contains(df_clean.review_clean, "good") ,1).otherwise(0)).select("cid", "id_review", "positive_review").withColumnRenamed("cid", "user_id")
    #df_out = df_out.withColumnRenamed("cid", "user_id").drop("review_clean")#funcionaba antes de agregar esto

    # parquet is a popular column storage format, we use it here
    #df_out.write.mode("overwrite").parquet(output_loc)
    #df_out.write.mode("overwrite").csv(output_loc)
    #df_out.write.csv("s3://staging-raquel/clean_data/movies_review.csv", mode='overwrite',header=True)
    df_out.write.csv("s3://staging-raquel/clean_data/movies_review/movies_review.csv", mode='overwrite',header=True)





def log_classifier(input_loc, output_loc):
    """
   description
    """

    # read input
    log_reviews = spark.read.option("header", True).option("inferSchema", "true").csv(input_loc)
    # perform text cleaning
    
    str_from_xml_udf = udf(lambda xml_str, path: get_str_from_xml(xml_str, path))

    log_reviews_clear = (
    log_reviews
      .select(
        col("*"),
        str_from_xml_udf(col("log"), lit("reviewlog.log.logDate")).alias("log_date"),
        str_from_xml_udf(col("log"), lit("reviewlog.log.device")).alias("device"),
        str_from_xml_udf(col("log"), lit("reviewlog.log.os")).alias("os"),  
        str_from_xml_udf(col("log"), lit("reviewlog.log.location")).alias("location"),
        str_from_xml_udf(col("log"), lit("reviewlog.log.ipAddress")).alias("ip"),
        str_from_xml_udf(col("log"), lit("reviewlog.log.phoneNumber")).alias("phone_number")
      )
    )
    
    df_out = log_reviews_clear.drop('log')
    
    # parquet is a popular column storage format, we use it here
    #df_out.write.mode("overwrite").parquet(output_loc)
    #df_out.write.format("csv").mode("overwrite").save("s3://staging-raquel/clean_data/log_review.csv")
    df_out.write.csv("s3://staging-raquel/clean_data/log_review/log_review.csv", mode='overwrite',header=True)


if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--input", type=str, help="HDFS input", default="/movie")
    #parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    #args = parser.parse_args()
    #spark = spark.sparkContext.setLogLevel("WARN")
    input_movies = "s3://raw-data-raquel/data/movie_review.csv"
    output_movies = "s3://staging-raquel/clean_data/movies_review"
    input_log = "s3://raw-data-raquel/data/log_reviews.csv"
    output_log = "s3://staging-raquel/clean_data/log_review"
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    movies_classifier(input_loc=input_movies, output_loc=output_movies)
    log_classifier(input_loc=input_log, output_loc=output_log)