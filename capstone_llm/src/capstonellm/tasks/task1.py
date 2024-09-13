from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

# Read the JSON file
df = spark.read.json('/workspace/capstone-llm/answers.json')

# Show the DataFrame
df.show()

df1 = spark.read.json('/workspace/capstone-llm/questions.json')
df1.show()

print.Schema(df)