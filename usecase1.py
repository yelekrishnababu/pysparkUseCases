import os
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
path="/home/ky2910/PycharmProjects/pysparkUseCases/addresses.csv"
name, extension = os.path.splitext(path)

if extension==".csv":
    df1 = spark.read.csv(path)
    df1.write.mode('overwrite').parquet("/home/ky2910/PycharmProjects/pysparkUseCases/results/test")
    df1 = spark.read.csv(path)
    # df1.write.avro("/home/ky2910/PycharmProjects/pysparkUseCases/results/testavro")
    # df1.select("*").write.format('avro').mode('overwrite').save('/home/ky2910/PycharmProjects/pysparkUseCases/results/testavro')
    # df1 = spark.read.csv(path)
    df1.write.mode('overwrite').json("/home/ky2910/PycharmProjects/pysparkUseCases/results/testjson")
