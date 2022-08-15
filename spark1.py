from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, month, year, current_timestamp, udf
from pyspark.sql.types import IntegerType


def dategen(day, month, year):
    strdate = str(day) + '-' + str(month) + '-' + str(year)
    return to_date(strdate, "dd-mm-yyyy")


def nullshadnling(comm):
    com = 0
    if comm == None:
        com = 0
    else:
        com = comm
    return com


def increments(sal,comm):
    newcomm = 0
    newsal = 0
    grade = salgrade(sal)
    print(sal)
    print(grade)
    if grade == 1:
        newsal = sal + (sal * 0.2)
        newcomm = comm + (sal*0.002)
    elif grade == 2:
        newsal = sal + (sal * 0.1)
        newcomm = comm + (sal * 0.0015)

    elif grade == 3:
        newsal = sal + (sal * 0.05)
        newcomm = comm + (sal * 0.0001)

    elif grade >= 4:
        newsal = sal + (sal * 0.002)
        newcomm = comm + (sal * 0.0001)

    print(int(newcomm))
    print(int(newsal))
    print([int(newsal),int(newcomm)])
    return [int(newsal),int(newcomm)]


def salgrade(sal):
    grade = 0
    if (sal < 0):
        print("Sal value can't be negative")
    elif (sal > 0) and (sal <= 1000):
        grade = 1
    elif (sal > 1000) and (sal <= 3000):
        grade = 2
    elif (sal > 3000) and (sal <= 5000):
        grade = 3
    else:
        grade = 4
    return grade


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

    empdf.select(expr("sum(sal)")).show()
    empdf.selectExpr("sum(sal)").show()
    empdf.selectExpr("ename", "deptno", "sal*2", "sal+comm").show()
    empdf.select("ename", "deptno", expr("sal+coalesce(comm,100)").alias("totalsal")).show(truncate=False)
    empdf.select(empdf.ename, empdf.sal).show(5, truncate=False)
    empdf.select(col("ename").alias("Employee Name"), "sal", "deptno").show()

    empdf2 = empdf.select("ename", "sal", empdf.deptno, to_date("hiredate", 'dd-mm-yyyy').alias("newdate"))

    empdf2.printSchema()

    empdf2.select(col("ename").alias("Employee Name"), month("newdate"), year("newdate"),
                  current_timestamp().alias("timenow")).show()

    empyr = empdf2.withColumn("year", year("newdate"))

    empyr.groupby("year").count().show(truncate=False)

    dates = empdf2.select(expr("01").alias("day"), month("newdate").alias("month"), year("newdate").alias("year"))

    dates.printSchema()
    dates.show()
    # udf Registration
    salgradeudf = udf(salgrade, IntegerType())
    commnull = udf(nullshadnling, IntegerType())
    empdf.withColumn("salgrade", salgradeudf(empdf.sal)) \
        .withColumn("newcomm", commnull(empdf.comm)).show()

    spark.udf.register("salgradeud", salgrade, IntegerType())
    spark.udf.register("comnull", nullshadnling, IntegerType())

    nullempdf = empdf.withColumn("salgrd", expr("salgradeud(sal)")).withColumn("commnull", expr("comnull(comm)"))

    increments = udf(increments, IntegerType())
    # spark.udf.register("salhike",increments, IntegerType())
    # nullempdf.withColumn("newsal", expr("increments(sal)")).show()
    # .withColumn("newcomm",increments(empdf.sal,empdf.comm)[1]).show()

    increments(5000,100)


