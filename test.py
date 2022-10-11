from openpyxl import Workbook
from openpyxl.reader.excel import load_workbook
from pyspark.sql import SparkSession, DataFrameWriter
jdbcHostname = "sqlserver045.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "ky2910"
properties = {
 "user" : "ky2910@sqlserver045",
 "password" : "Krishna@2000" }
url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

wb=Workbook()
filePath="/home/ky2910/PycharmProjects/pysparkUseCases/inputs/test.xlsx"
wb=load_workbook(filePath)
for i in wb.sheetnames:
    print(i)
    df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("treatEmptyValuesAsNulls", "false")\
            .option("dataAddress", f"'{i}'!A1") \
        .option("treatEmptyValuesAsNulls", "false")\
            .load(filePath)
    myfinaldf = DataFrameWriter(df)
    myfinaldf.jdbc(url=url, table= i, mode ="overwrite", properties = properties)
