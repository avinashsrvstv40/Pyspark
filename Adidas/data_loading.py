from pyspark.sql import SparkSession
import sys
from lib.util import get_spark_config, load_book_data, filtered_data_table
from pyspark.sql.functions import col, count, regexp_replace, explode_outer, expr, lit, current_timestamp, date_format
from lib.logger import log4j


# Parsing json data to get values from necessary columns using explode_outer for exploding array type
def transformed_df(books_info):
    transformed_emp = books_info.withColumn("languages", explode_outer("languages")) \
        .withColumn("subjects", explode_outer("subjects")) \
        .withColumn("other_titles", explode_outer("other_titles")) \
        .withColumn("publishers", explode_outer("publishers")) \
        .withColumn("authors", explode_outer("authors")) \
        .withColumn("genres", explode_outer("genres")) \
        .withColumn("publish_places", explode_outer("publish_places")) \
        .withColumn("download_url", explode_outer("download_url")) \
        .withColumn("batch_date", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))) \
        .select("Title", regexp_replace(expr("languages.key"), "/languages/", "").alias("languages"),
                "subjects", regexp_replace(expr("location"), "[\[\]\"]", "").alias("location"),
                "other_titles", "publishers", "publish_places", col("last_modified.value").alias("last_modified"),
                regexp_replace(expr("authors.key"), "/authors/", "").alias("authors")
                , col("created.value").alias("created"), "genres", "contributions", "number_of_pages",
                "publish_country", "publish_date", "download_url", "batch_date")

    return transformed_emp


# Filtering data based on some conditions
def filtered_data(transformed_emp):
    filtered_emp = transformed_emp.where((col("Title").isNotNull()) & (col("number_of_pages") > 20)
                                         & (col("publish_date") > 1950)
                                         & (col("genres").isNotNull()))
    return filtered_emp


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise Exception("Enter data file")

    conf = get_spark_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = log4j(spark)

    logger.info("Data loading job has started")

    books_info_df = load_book_data(spark, sys.argv[1])

    logger.info("Source count is " + str(books_info_df.count()))

    logger.info("Printing the schema of raw json data")
    books_info_df.printSchema()

    books_info_df.createOrReplaceTempView("books_info")

    spark.sql("select distinct publish_date from books_info").show(truncate=False)

    logger.info("Executing transformed_df function")
    transformed_emp_df = transformed_df(books_info_df)

    logger.info("Executing filtered_data function")
    filtered_emp_df = filtered_data(transformed_emp_df)

    # Loading transformed final table into table book_info

    spark.sql("create database if not exists Adidas")

    logger.info("Loading filtered data into table Adidas.books_info")
    filtered_data_table(filtered_emp_df)

    logger.info("Data loading job has been completed successfully")

    logger.info("Target count of final table is " + str(spark.table('Adidas.books_info').count()))

    #  *****************************Below are 5 queries *****************************

    logger.info("Running queries on the loaded data")

    long_book = filtered_emp_df.orderBy(col("number_of_pages").desc()).show(5, truncate=False)

    top_5_genres = filtered_emp_df.groupBy("genres").agg(count("*").alias("num_of_books")) \
        .orderBy(col("num_of_books").desc()).select("genres").show(5, truncate=False)

    top_5_contributors = filtered_emp_df.where(col("contributions").isNotNull()) \
        .groupBy("contributions") \
        .agg(count("*").alias("num_of_books")) \
        .orderBy(col("num_of_books").desc()).show(5, truncate=False)

    num_authors = filtered_emp_df.groupBy("publish_date", "authors").agg(count("*").alias("num_of_books")) \
        .where(col("num_of_books") == 1).show(5, truncate=False)

    filtered_emp_df.where(col("publish_date").between(1950, 1970)).show(5)

    logger.info("All queries has been executed successfully")
