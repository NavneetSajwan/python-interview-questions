from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

print(spark)

sc=spark._sc

input_file=sc.textFile("../data/cover_letter.txt")

for item in input_file.collect():
    print(item)