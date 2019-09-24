from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructType, StructField
from pyspark.sql.functions import expr, desc, first, col, asc, last, when, reverse
from pyspark.sql.window import Window
from pyspark.sql import types as t
from pyspark.sql import Row
from pyspark.sql.window import Window
import sys
import re
import os

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL basic example")\
    .getOrCreate()

spark.conf.set('spark.driver.memory', '6g')
spark.conf.set('spark.sql.session.timeZone', 'Europe/Berlin')

sessions = [
        Row(1, "20-Sep-2019", "adwords"), 
        Row(1, "21-Sep-2019", "(direct)"), 
        Row(1, "22-Sep-2019", "(direct)"), 
        Row(1, "23-Sep-2019", "(direct)")
        ]

new_sessions = [
        Row(1, "24-Sep-2019", "newsletter"),
        Row(1, "25-Sep-2019", "(direct)")
        ]

sessionsDF = spark.createDataFrame(sessions, schema=["session_id", "timestamp", "source"])
new_sessionsDF = spark.createDataFrame(new_sessions, schema=["session_id", "timestamp", "source"])
print("\n")
print("DATAFRAME SESSIONS YESTERDAY")
sessionsDF.show()
print("\n")
print("DATAFRAME SESSIONS TODAY")
new_sessionsDF.show()

united = sessionsDF.union(new_sessionsDF)

windowSpec = Window\
        .partitionBy("session_id")\
        .orderBy("timestamp")\

first_touchpoint = first(col("source")).over(windowSpec)

print("DATAFRAME UNIONED YESTERDAY + TODAY WITH TOUCHPOINTS")
united.selectExpr("*",
        "collect_list(source) over (partition by session_id) as touchpoints")\
      .withColumn("touchpoints_wo_direct", expr("filter(touchpoints, x -> x != '(direct)')"))\
      .orderBy("timestamp")\
      .select("*",
              first_touchpoint.alias("first_touchtpoint"), 
              when(reverse(col("touchpoints_wo_direct"))[0].isNotNull(), reverse(col("touchpoints_wo_direct"))[0]).otherwise("(direct)").alias("last_touchpoint"))\
      .show(truncate=False)


