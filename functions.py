from pyspark.sql.functions import col, sum, avg, max, min, to_date


def loadempdf(spark,datafile):

    return spark.read.format("csv").options(header = True, inferSchema = True).load(datafile)

def totalsal(dataframe):
    total_sal = dataframe.withColumn("totalsal",col("sal")+col("comm"))
    return total_sal

def deptagg(dataframe):
    aggdata = dataframe.groupBy(col("deptno")).agg(sum(col("sal")).alias("totalsal"),
                                          avg(col("sal")).alias("avgsal"),
                                          max(col("sal")).alias("maxsal"),
                                          min("sal").alias("minsal"))

    return aggdata



