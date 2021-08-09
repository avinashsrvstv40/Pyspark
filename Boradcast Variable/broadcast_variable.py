from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap
from pyspark.sql.types import IntegerType
from lib.util import get_spark_config


def state_convert(code):
    return broadcastStates.value[code]


if __name__ == "__main__":
    conf = get_spark_config()

    spark = SparkSession.builder. \
        config(conf=conf).getOrCreate()

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcastStates = spark.sparkContext.broadcast(states)

    employee_df = spark.read.option("header", "true") \
        .csv("C:\\Users\\user\\Downloads\\5000-Records\\5000_Records.csv") \
        .withColumnRenamed("Age in Yrs.", "Age") \
        .withColumn("Age", col("Age").cast(IntegerType())) \
        .withColumn("First Name", initcap(col("First Name"))) \
        .repartition(2)

    employee_df.printSchema()
    employee_df.show(truncate=False)

    # Broadcast variable on filter
    filteDf = employee_df.where((employee_df['State'].isin(list(broadcastStates.value.keys()))))

    filtered_df = filteDf.select("Emp ID", "First Name", "Last Name", "State") \
        .rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3])))

    spark.createDataFrame(filtered_df, schema=["Emp ID", "Firs tName", "Last Name",
                                               "State"]).show(False)