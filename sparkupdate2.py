from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark import SparkContext

def create_spark_session():
    spark = SparkSession.builder.appName("firstApp").getOrCreate()
    return spark

def load_data(spark):
    read_file = spark.read.csv(file_path, header=True)\
              .option("path","hdfs:///user/maria_dev/spark/linkdin_Job_data.csv").load()
    return read_file

def clean_data(data):
    data = data.drop("Column1", "company_id", "posted_day_ago")
    data = data.withColumn("alumni", F.regexp_extract("alumni", r'(\d+)', 1).cast(IntegerType())) \
        .withColumn("linkedin_followers", F.regexp_replace("linkedin_followers", r' followers', '').cast(IntegerType())) \
        .withColumn("no_of_application", F.col("no_of_application").cast('int'))
    data = data.na.drop()
    return data

def analyse_data(data):
    data.show(1)
    data.printSchema()
    summary_stats = data.describe(["no_of_employ", "no_of_application"])
    return summary_stats.show()

def transformations_rdd(data_rdd):
    sc = SparkContext.getOrCreate()
    data_rdd = sc.parallelize(data.rdd.collect(),5)
    partitions = data_rdd.getNumPartitions()
    print("Initial partition count:", partitions, "partitions")
    rdd1 = data_rdd.map(lambda x: (x[1], x[2], x[3]))
    rdd2 = rdd1.filter(lambda x: x[2] == "India")
    return rdd1,rdd2
    
def actions_rdd(data_rdd,rdd2):
    first_element = data_rdd.first()
    take_data = data_rdd.take(5)
    count_rdd = data_rdd.count()

    print("First element:", first_element)
    print("Total number of rdd elements:", count_rdd)

    output_path = "hdfs:///user/maria_dev/spark/output"
    rdd2.saveAsTextFile(output_path)

if __name__ == "__main__":
    spark_session = create_spark_session()

    data = load_data(spark_session)

    cleaned_data = clean_data(data)
    analyse_data(cleaned_data)

    data_rdd = cleaned_data.rdd

    rdd1,rdd2 = transformations_rdd(data_rdd)
    
    actions_rdd(data_rdd,rdd2)

