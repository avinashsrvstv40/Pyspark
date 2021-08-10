from pyspark import SparkConf
import configparser


def get_spark_config():
    conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for k, v in config.items("SPARK_APP_CONF"):
        conf.set(k, v)
    return conf


def load_employee_data(spark, data_file):
    return spark.read.option("header", "true") \
        .csv(data_file)
