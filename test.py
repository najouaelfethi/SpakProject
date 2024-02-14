# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ExampleApp").getOrCreate()


data = spark.read.csv("linkdin_Job_data.csv", header=True)


first_row = data.first()
print("Première ligne du dataset:", first_row)


output_path = "hdfs:///user/maria_dev/spark/output"
data.write.text(output_path)


spark.stop()
