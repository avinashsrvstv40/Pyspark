from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap
from pyspark.sql.types import IntegerType
from lib.util import get_spark_config, load_employee_data
import sys


def state_convert(code):
    return broadcastStates.value[code]


if __name__ == "__main__":
    conf = get_spark_config()

    spark = SparkSession.builder. \
        config(conf=conf).getOrCreate()

    if len(sys.argv) != 2:
        raise Exception("Require data file")

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcastStates = spark.sparkContext.broadcast(states)

    employee_df = load_employee_data(spark, sys.argv[1]) \
        .withColumnRenamed("Age in Yrs.", "Age") \
        .withColumn("Age", col("Age").cast(IntegerType())) \
        .withColumn("First Name", initcap(col("First Name"))) \
        .repartition(2)

    employee_df.printSchema()
    employee_df.show(truncate=False)

    # Broadcast variable on filter
    filterDf = employee_df.where((employee_df['State'].isin(list(broadcastStates.value.keys()))))

    filtered_df = filterDf.select("Emp ID", "First Name", "Last Name", "State") \
        .rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3])))

    spark.createDataFrame(filtered_df, schema=["Emp ID", "First Name", "Last Name", "State"]) \
        .show(truncate=False)
