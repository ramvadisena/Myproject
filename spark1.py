from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Employe data processing").master("local[4]").getOrCreate()

    empdf = spark.read.format("csv").load("file:///Users/ashrith/ram_spark/Datasets/emp.csv")

    empdf.show(5)