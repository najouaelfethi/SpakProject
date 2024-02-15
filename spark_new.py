from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StringType, StructField
from pyspark import SparkContext

spark = SparkSession.builder.appName("firstApplication").getOrCreate()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("path", "hdfs:///user/maria_dev/spark/linkdin_Job_data.csv") \
    .load()
print("Dataset contains:", df.count(), "rows")
df.printSchema()
data_cleaned = df.drop("Column1", "company_id", "posted_day_ago")
data_cleaned.printSchema()
data_cast = data_cleaned.withColumn("alumni", F.regexp_extract("alumni", r'(\d+)', 1).cast(IntegerType())) \
    .withColumn("linkedin_followers", F.regexp_replace("linkedin_followers", r' followers', '').cast(IntegerType())) \
    .withColumn("no_of_application", F.col("no_of_application").cast('int'))
data = data_cast.na.drop()
data.printSchema()
summary_stats = data.describe(["no_of_employ", "no_of_application"])
summary_stats.show()
sc = SparkContext.getOrCreate()
data_rdd = sc.parallelize(data.collect())
try:
    rdd1 = data_rdd.map(lambda x: (x[1], x[2], x[3]))
    print("rdd1 created")
except Exception as e:
    print("Error creating RDD:", str(e))
try:
    rdd1.repartition(50).saveAsTextFile("hdfs:///user/maria_dev/spark/output")
    print("RDD saved successfully")
except Exception as e:
    print("Error saving RDD:", str(e))
spark.stop()
sc.stop()
