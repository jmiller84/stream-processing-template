from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToPySpark").getOrCreate()

data = [("John", "Doe", 29), ("Jane", "Smith", 34), ("Sam", "Brown", 23)]
columns = ["first_name", "last_name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

# Read csv file
df_from_csv = spark.read.csv("/home/ec2-user/stream-processing-template/my-files/phase01/test_csv.csv", header=True, inferSchema=True)
df_from_csv.show()

# Selecting specific columns
df.select("first_name", "age").show()

# Filtering data
df.filter(df["age"] > 25).show()

# Grouping and Aggregating
df.groupBy("age").count().show()