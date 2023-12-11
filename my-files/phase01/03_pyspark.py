from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToPySpark").getOrCreate()

data = [("John", "Doe", 29), ("Jane", "Smith", 34), ("Sam", "Brown", 23)]
columns = ["first_name", "last_name", "age"]

df = spark.createDataFrame(data, columns)
df.show()