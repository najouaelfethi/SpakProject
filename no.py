from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

FILE_PATH = "hdfs:///user/maria_dev/spark/linkdin_Job_data.csv"
OUTPUT_PATH = "hdfs:///user/maria_dev/spark/output"

def create_spark_session():
    return SparkSession.builder.appName("firstApp").getOrCreate()

def load_data(spark, file_path):
    return spark.read.csv(file_path, header=True)

def clean_data(data):
    cleaned_data = data.drop("Column1", "company_id", "posted_day_ago")
    cleaned_data = cleaned_data.withColumn("alumni", F.regexp_extract("alumni", r'(\d+)', 1).cast(IntegerType()))
    cleaned_data = cleaned_data.withColumn("linkedin_followers", F.regexp_replace("linkedin_followers", r' followers', '').cast(IntegerType()))
    cleaned_data = cleaned_data.withColumn("no_of_application", F.col("no_of_application").cast('int'))
    return cleaned_data.na.drop()

def analyse_data(data):
    data.show(1)
    data.printSchema()
    summary_stats = data.describe(["no_of_employ", "no_of_application"])
    summary_stats.show()

def save_rdd_to_hdfs(rdd, output_path):
    rdd.saveAsTextFile(output_path)

if __name__ == "__main__":
    spark_session = create_spark_session()
    data = load_data(spark_session, FILE_PATH)
    cleaned_data = clean_data(data)
    analyse_data(cleaned_data)
    data_rdd = cleaned_data.rdd

    rdd1 = data_rdd.map(lambda x: (x[1], x[2], x[3]))
    rdd2 = rdd1.filter(lambda x: x[2] == "India")
    save_rdd_to_hdfs(rdd2, OUTPUT_PATH)

    spark_session.stop()
