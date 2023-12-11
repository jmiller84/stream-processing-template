from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToPySpark").getOrCreate()

data = [("John", "Doe", 29), ("Jane", "Smith", 34), ("Sam", "Brown", 23)]
columns = ["first_name", "last_name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

df_from_csv = spark.read.csv("my-files/phase01/03_pyspark.py", header=True, inferSchema=True)
df_from_csv.show()