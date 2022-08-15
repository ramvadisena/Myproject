from unittest import TestCase

from pyspark.sql import SparkSession


class emptestbaseclass(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Testcase") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
