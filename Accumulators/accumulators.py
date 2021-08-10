from pyspark.sql import SparkSession
from lib.util import get_spark_config, load_employee_data
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import sys


def count_age_above_50(row):
    Age = row.Age
    if Age > 50:
        counter.add(1)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise Exception("Require Data File")

    conf = get_spark_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    counter = spark.sparkContext.accumulator(0)

    employee_df = load_employee_data(spark, sys.argv[1]) \
        .withColumnRenamed("Age in Yrs.", "Age") \
        .withColumn("Age", col("Age").cast(IntegerType())) \
        .where("Age is not null")

    employee_df.printSchema()

    employee_df.foreach(lambda x: count_age_above_50(x))

    print(f"The no. of people above 50 are {counter}")
