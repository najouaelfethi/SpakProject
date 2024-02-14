# -- coding: utf-8 --
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import *
import pyspark.sql.functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import findspark
findspark.init()

import os
os.environ['SPARK_HOME'] = "C:/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

# Create Spark Session
spark = SparkSession.builder.appName("firstApp").getOrCreate()

spark

data = spark.read.csv("linkdin_Job_data.csv", header='True')

data.show(1) #shows first 20 rows

#numbers of rows
print("dataSet contains:",data.count(),"rows") 

data.printSchema() #meta data of dataframe

#drop: column1,job_details
data = data.drop("Column1", "company_id", "posted_day_ago")

data.show(1)

data.printSchema()

# Convert some columns into numeric
data = data.withColumn("alumni",regexp_extract("alumni",r'(\d+)',1).cast(IntegerType()))\
.withColumn("linkedin_followers", regexp_replace("linkedin_followers",r' followers', ''))\
.withColumn("no_of_application",col("no_of_application").cast('int'))

data.printSchema()

data.show()

# Handling Missing Values

#Missing values
data = data.na.drop()

data.show(735)

data.count()

data.printSchema()

# Description of statistics

# Show summary statistics for numeric columns
summary_stats = data.describe(["no_of_employ", "no_of_application"])
summary_stats.show()

### Create RDD
sc = SparkContext.getOrCreate()
data_rdd = sc.parallelize(data.rdd.collect(),5) #dataset is distributed into 4 partitions(clusters)
data_rdd

partitions = data_rdd.getNumPartitions()
print("initial partition count:",partitions,"partitions")

### RDD Operation: Transformation

# Création d'une RDD contenant les colonnes job et location
rdd1 = data_rdd.map(lambda x: (x.job_ID, x.job, x.location))
rdd1.collect()

# Filtrer les locations égales à 'India'
rdd2 = rdd1.filter(lambda x: x[2] == "India")
rdd2.collect()

rdd3 = data_rdd.filter(lambda x: x.full_time_remote == "Full-time").map(lambda x: (x.job_ID, x.full_time_remote)).distinct()
#rdd3 = rdd2.map(lambda x : (x.job_ID, x.job))
rdd3.collect()
# ### RDD Operation: Action
#Get first element
first_element = data_rdd.first()
print(first_element)

#Retrieve n element using take()
take_data = data_rdd.take(5)
take_data

#Count: return total number of RDD
count_rdd = data_rdd.count()
print("Total number of rdd elements: ",count_rdd)

### Save RDDs outputs to text file and read from it
output_path = "hdfs:///user/mari_dev/output"
rdd2.saveAsTextFile(output_path)
