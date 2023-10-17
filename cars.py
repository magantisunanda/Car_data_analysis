from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CarDataAnalysis").getOrCreate()
#loading data
df = spark.read.csv(r'C:\Users\kbpav\Downloads\prices_clean.csv', header=True, inferSchema=True)

# Importing required functions
from pyspark.sql.functions import col, max, avg, countDistinct

df1 = df.alias("df1")

# maximum price for each group
max_prices = df.groupBy("make").agg(max("price").alias("max_price"))
max_price = max_prices.alias("max_price")

# Join df with max_prices on 'make' and 'price'
result1 = df1.join(max_price, (col("df1.make") == col("max_price.make")) & (col("df1.price") == col("max_price.max_price")), how='inner')

# Q1 models in every make with highest price.
result1 = result1.select(col("df1.make"), col("df1.model"), col("df1.price"))

# Show the result
print("models in every make with highest price:")
result1.show()

max_prices = df.groupBy("make", "year").agg(max("price").alias("max_price"))
max_price = max_prices.alias("max_price")
#Q2. models in every make every year with highest price
result2 = df1.join(max_price, (col("df1.make") == col("max_price.make")) & (col("df1.year") == col("max_price.year")) & (col("df1.price") == col("max_price.max_price")), how='inner')
result2 = result2.select(col("df1.make"), col("df1.year"), col("df1.model"), col("df1.price"))
print("models in every make every year with highest price:")
result2.show()
avg_prices = df.groupBy("make").agg(avg("price").alias("avg_price"))
df_with_avg = df.join(avg_prices, "make")
result3 = df_with_avg.filter(col("price") > col("avg_price"))

#Q3. models whose price is greater than the average price by make
result3 = result3.select(col("make"), col("model"), col("price"))
print("models whose price is greater than the average price by make:")
result3.show()

#Q4. number of models released by each maker
result4 = df.groupBy("make").agg(countDistinct("model").alias("num_models"))
print("number of models released by each maker:")
result4.show()



