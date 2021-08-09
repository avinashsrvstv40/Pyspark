from pyspark import SparkConf
from configparser import ConfigParser


def get_spark_config():
    spark_conf = SparkConf()
    config = ConfigParser()
    config.read("spark.conf")

    for key, val in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(key, val)
    return spark_conf


def load_employee_data(spark, data_file):
    return spark.read.option("header", "true") \
        .csv(data_file)
