from __future__ import annotations

import os
import sys
from datetime import datetime, date
from logging import getLogger, StreamHandler

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SparkSession


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.0,com.google.guava:guava:27.1-jre --properties-file s3.properties pyspark-shell'


class MyClass(object):
    def func(self, s):
        return s + 5
    def doStuff(self, rdd):
        return rdd.map(self.func)


def pra_1(sc: SparkContext, slice_num: int) -> None:
    mc = MyClass()
    rdd = sc.parallelize(range(1, 50), slice_num)
    rdd_2 = mc.doStuff(rdd)
    res = rdd_2.reduce(lambda l, r: l + r)
    print(res)


def pra_2(sc: SparkContext) -> None:
    accum = sc.accumulator(0)
    sc.parallelize(range(1,50)).foreach(lambda x: accum.add(x))
    print(accum.value)


def pra_3(sc: SparkContext) -> None:
    conf = sc.getConf()
    spark = SparkSession.builder.appName("myApp").config(conf=conf).getOrCreate()
    df = spark.createDataFrame([
        ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
        ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
        ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
    df.createOrReplaceTempView("my_data")
    sqlDF = spark.sql("SELECT color, COUNT(1) FROM my_data GROUP BY color")
    sqlDF.show()


def pra_4(sc: SparkContext) -> None:
    conf = sc.getConf()
    spark = SparkSession.builder.appName("myApp").config(conf=conf).getOrCreate()
    squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda i: Row(single=i, double=i ** 2)))
    squaresDF.write.format("parquet").mode("overwrite").save("s3a://uchimura-test-bucket/data/test_table/key=1")
    cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11)).map(lambda i: Row(single=i, triple=i ** 3)))
    cubesDF.write.format("parquet").mode("overwrite").save("s3a://uchimura-test-bucket/data/test_table/key=2")

    mergedDF = spark.read.option("mergeSchema", "true").parquet("s3a://uchimura-test-bucket/data/test_table")
    mergedDF.printSchema()
    mergedDF.createOrReplaceTempView("tmpv")
    spark.sql("SELECT * FROM tmpv ORDER BY single").show()
    import pdb;pdb.set_trace()


if __name__ == "__main__":
    # conf = SparkConf().setAppName("myApp").setMaster("spark://localhost:7077")
    conf = SparkConf().setAppName("myApp").setMaster("local")
    sc = SparkContext(conf=conf)

    pra_4(sc)
