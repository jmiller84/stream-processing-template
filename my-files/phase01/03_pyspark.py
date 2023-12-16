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

# Using RDDs
df.createOrReplaceTempView("people")
result = spark.sql("SELECT * FROM people WHERE age > 25")
result.show()

# Machine Learning with Pyspark MLlib
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

# Sample data
data = [(Vectors.dense([0.0]), 1.0),
        (Vectors.dense([1.0]), 2.0),
        (Vectors.dense([2.0]), 3.0)]

df = spark.createDataFrame(data, ["features", "label"])

# Linear Regression model
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(df)

# Print the coefficients
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))
