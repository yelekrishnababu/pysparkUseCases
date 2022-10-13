from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, flatten
from pyspark.sql import Row

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
readComplexJSONDF = spark.read.option("multiLine", "true").json('inputs/test.json')
# readComplexJSONDF.select(readComplexJSONDF.array[0]).show()
df=readComplexJSONDF.select((explode(readComplexJSONDF.array))).collect()
test_list=[]
for i in range(len(df)):
    test_list.append(Row(df[i][0][0],df[i][0][1]))
final_df=spark.createDataFrame(test_list,['name','age'])
final_df.show(truncate=False)


# readComplexJSONDF.show()
#
# # using schema
# json=[("""
# {
#   "name":
#     {
#       "Full":
#       {
#         "First":"Krishna",
#         "Middle":"Babu"
#       }
#
#     },
#       "Last":"Yele"
# }
# """)]
# schema = StructType(
#     [
#         StructField("Last", StringType(), True),
#         StructField("name", StructType(
#             [StructField("Full", StructType(
#                 [StructField("First", StringType(), True), StructField("Middle", StringType(), True)]), True)]), True)
#     ]
# )
# schema=StructType(
#     StructField("array",)
# )
# readComplexJSONDF = spark.read.option("multiLine","true").json('inputs/test.json')
# readComplexJSONDF.printSchema()
# readComplexJSONDF.select("name.Full.Middle").show()
# #
#
# # df=spark.createDataFrame([(1,"""
# #  "name":
# #     {
# #       "Full":{"First":"Krishna","Middle":"Babu"}
# #     },
# #       "Last":"Yele"
# # }
# # """
# # )],[("jsonData","value")])
# # # df.printSchema()
# # dfJSON = df.withColumn("jsonData",from_json(col("value"),schema)) \
# #                    .select("jsonData.*")
# # dfJSON.show(truncate=False)
# # data=
#
# df=spark.createDataFrame(json,["value"])
# df.show()
# # dfJSON = df.withColumn("jsonData",from_json(col("value"),schema)) \
# #                    .select("jsonData.*")
# # df.show()
from pyspark import Row
# from pyspark.sql.functions import from_json, col
#
#
# jstr1 = u'{"header":{"id":12345,"foo":"bar"},"body":{"id":111000,"name":"foobar","sub_json":{"id":54321,"sub_sub_json":{"col1":20,"col2":"somethong"}}}}'
# jstr2 = u'{"header":{"id":12346,"foo":"baz"},"body":{"id":111002,"name":"barfoo","sub_json":{"id":23456,"sub_sub_json":{"col1":30,"col2":"something else"}}}}'
# jstr3 = u'{"header":{"id":43256,"foo":"foobaz"},"body":{"id":20192,"name":"bazbar","sub_json":{"id":39283,"sub_sub_json":{"col1":50,"col2":"another thing"}}}}'
# df = spark.createDataFrame([Row(json=jstr1),Row(json=jstr2),Row(json=jstr3)])
# json_schema = spark.read.json(df.rdd.map(lambda row: row.json)).schema
#
# df.withColumn('json', from_json(col('json'), json_schema)).show(truncate=False)
# df_with_schema = spark.read.option("multiLine","true").schema(schema).json("inputs/test.json")
# df_with_schema.printSchema()
# df_with_schema.select("name.Full.First").show()
# df_with_schema.select(df_with_schema.Last,explode(df_with_schema.name)).show()