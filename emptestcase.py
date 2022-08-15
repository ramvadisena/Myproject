from emptestbaseclass import emptestbaseclass
from Empdata import *

class Emptest(emptestbaseclass):
    def test_datacount(self):
        self.df = self.spark.read.format("csv") \
            .options(header=True, inferSchema=True) \
            .load("file:///Users/ashrith/ram_spark/Datasets/emp.csv")

        expecteddatacount = 15
        datacountfunc = Empdata().rowcount(self.df)
        self.assertEqual(datacountfunc, expecteddatacount)

    def test_totalsaldept(self):
        df2 =self.df
        aggdf = Empdata().aggvals(df2,"deptno","sal","totalsal")
        aggdf.show()
        aggdf.sort('deptno').toPandas()

        outputvalues = aggdf.to_dict("list")["totalsal"][0]




