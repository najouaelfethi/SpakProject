# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark import SparkContext

import findspark
findspark.init()

# Create Spark Session
spark = SparkSession.builder.appName("firstApp").getOrCreate()

data = spark.read.csv("linkdin_Job_data.csv", header=True)

data.show(1)  # shows first row

# number of rows
print("Dataset contains:", data.count(), "rows")

data.printSchema()  # metadata of dataframe

# drop: column1, company_id, posted_day_ago
data = data.drop("Column1", "company_id", "posted_day_ago")

data.show(1)

data.printSchema()

# Convert some columns into numeric
data = data.withColumn("alumni", F.regexp_extract("alumni", r'(\d+)', 1).cast(IntegerType())) \
    .withColumn("linkedin_followers", F.regexp_replace("linkedin_followers", r' followers', '').cast(IntegerType())) \
    .withColumn("no_of_application", F.col("no_of_application").cast('int'))

data.printSchema()

data.show()

# Handling Missing Values

# Missing values
data = data.na.drop()

data.show(735)

data.printSchema()

# Description of statistics

# Show summary statistics for numeric columns
summary_stats = data.describe(["no_of_employ", "no_of_application"])
summary_stats.show()

### Create RDD
sc = SparkContext.getOrCreate()
data_rdd = sc.parallelize(data.collect(), 5)  # dataset is distributed into 5 partitions

partitions = data_rdd.getNumPartitions()
print("initial partition count:", partitions, "partitions")

### RDD Operation: Transformation

# Création d'une RDD contenant les colonnes job et location
rdd1 = data_rdd.map(lambda x: (x[1], x[2], x[3]))

rdd1.collect()

# Filtrer les locations égales à 'India'
rdd2 = rdd1.filter(lambda x: x[2] == "India")
rdd2.collect()

rdd3 = data_rdd.filter(lambda x: x[5] == "Full-time").map(lambda x: (x[1], x[5])).distinct()
rdd3.collect()

### RDD Operation: Action

# Get first element
first_element = data_rdd.first()
print(first_element)

# Retrieve n element using take()
take_data = data_rdd.take(5)
take_data

# Count: return total number of RDD
count_rdd = data_rdd.count()
print("Total number of rdd elements: ", count_rdd)

### Save RDDs outputs to text file and read from it
output_path = "hdfs:///user/maria_dev/spark/output"
rdd2.saveAsTextFile(output_path)
