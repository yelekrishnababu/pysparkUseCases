from pyspark.sql import SparkSession, DataFrameWriter
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
df = spark.read.csv("/home/ky2910/PycharmProjects/pysparkUseCases/file1csv.csv.gz", header=True)
jdbcHostname = "w3.training5.modak.com"
jdbcPort = "5432"
jdbcDatabase = "training"
properties = {
 "user" : "mt4038",
 "password" : "mt4038@m02y22",
"driver": "org.postgresql.Driver"
}
url = "jdbc:postgresql://{0}:{1}/{2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
df.write.format("jdbc").mode("overwrite").option("url","jdbc:postgresql://w3.training5.modak.com:5432/training").option(
 "driver", "org.postgresql.Driver").option("dbtable", "testKY2910").option("user","mt4038").option("password", "mt4038@m02y22").save()
# df.write.jdbc(url=url, table= "test_kY2910", mode ="overwrite", properties = properties)
# myfinaldf.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://w3.training5.modak.com:5432/training") \
#     .option("dbtable", "testKY2910") \
#     .option("user", "mt4038") \
#     .option("password", "mt4038@m02y22") \
#     .option("driver", "org.postgresql.Driver") \
#     .save()
