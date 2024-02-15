from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark import SparkContext

import findspark
findspark.init()

spark = SparkSession.builder.appName("firstApp").getOrCreate()

df = spark.read.csv("linkdin_Job_data.csv", header=True)


print("Dataset contains:", df.count(), "rows")

print(df.printSchema())

data_cleaned = df.drop("Column1", "company_id", "posted_day_ago")

data_cleaned.printSchema()

data_cast = data_cleaned.withColumn("alumni", F.regexp_extract("alumni", r'(\d+)', 1).cast(IntegerType())) \
    .withColumn("linkedin_followers", F.regexp_replace("linkedin_followers", r' followers', '').cast(IntegerType())) \
    .withColumn("no_of_application", F.col("no_of_application").cast('int'))

data = data_cast.na.drop()

data.printSchema()


summary_stats = data.describe(["no_of_employ", "no_of_application"])
summary_stats.show()

data_rdds = data.rdd
data_rdd = data_rdds.repartition(200)
partitions = data_rdd.getNumPartitions()
print("initial partition count:", partitions, "partitions")
try:
   rdd1 = data_rdd.map(lambda x: (x[1], x[2], x[3])).filter(lambda x: x[2] == "India")
   print("rdd1 created")
except:
    print("rdd1 not created")

output_path = "hdfs:///user/maria_dev/spark/output"
try:
    rdd1.saveAsTextFile(output_path)
except Exception as e:
    print("Error saving RDD")

spark.stop()

