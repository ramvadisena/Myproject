import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date,sum

from functions import loadempdf, totalsal, deptagg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Employe data processing").master("local[4]").getOrCreate()

    empdf = spark.read.format("csv").options(header=True, inferSchema=True) \
        .load("file:///Users/ashrith/ram_spark/Datasets/emp.csv")
    # empdf = loadempdf(spark, sys.argv[1])
    empdf.printSchema()
    # column string operations
    empdf.select("ename", "sal", "deptno").show(5)
    # empdf.select("ename,sal,deptno").show(5)
    empdf.createOrReplaceTempView("emp")
    sal = spark.sql("select sum(sal) from emp").show()

    newempdf = empdf.withColumn("hiredate", to_date(col("hiredate"), "dd/mm/yyyy"))

    sumdf = empdf.groupBy("deptno").agg(sum("sal").alias("sumsal"))

    print(type(sumdf))

