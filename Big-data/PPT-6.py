#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 1. Working with RDDs:
#    a) Write a Python program to create an RDD from a local data source.
#    b) Implement transformations and actions on the RDD to perform data processing tasks.
#    c) Analyze and manipulate data using RDD operations such as map, filter, reduce, or aggregate.


# In[ ]:


from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local[*]", "spark1")

# Create an RDD from a local data source
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Perform transformations and actions on the RDD
squared_rdd = rdd.map(lambda x: x ** 2)  # Square each element
filtered_rdd = squared_rdd.filter(lambda x: x > 10)  # Filter elements greater than 10
sum_of_elements = filtered_rdd.reduce(lambda x, y: x + y)  # Reduce to calculate the sum

# Print the RDD and the result
print("Original RDD:")
for element in rdd.collect():
    print(element)

print("\nTransformed RDD:")
for element in squared_rdd.collect():
    print(element)

print("\nFiltered RDD:")
for element in filtered_rdd.collect():
    print(element)

print("\nSum of elements:", sum_of_elements)


# In[ ]:


# 2. Spark DataFrame Operations:
#    a) Write a Python program to load a CSV file into a Spark DataFrame.
#    b)Perform common DataFrame operations such as filtering, grouping, or joining.
#    c) Apply Spark SQL queries on the DataFrame to extract insights from the data.


# In[ ]:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("spark2").getOrCreate()

# Load a CSV file into a DataFrame
df = spark.read.csv("C:\Users\asus\Downloads/employee.csv", header=True, inferSchema=True)

# Display the DataFrame schema
df.printSchema()

# Perform filtering
filtered_df = df.filter(df["age"] > 25)

# Perform grouping and aggregation
grouped_df = df.groupBy("gender").agg({"salary": "avg"})

# Perform joining with another DataFrame
other_df = spark.read.csv("C:\Users\asus\Downloads/department.csv", header=True, inferSchema=True)
joined_df = df.join(other_df, "deptid")

# Apply Spark SQL queries on the DataFrame
df.createOrReplaceTempView("employees")
result = spark.sql("SELECT * FROM employees WHERE salary > 50000")

# Display the results
filtered_df.show()
grouped_df.show()
joined_df.show()
result.show()

# Stop the SparkSession
spark.stop()


# In[ ]:


# 3. Spark Streaming:
#   a) Write a Python program to create a Spark Streaming application.
#    b) Configure the application to consume data from a streaming source (e.g., Kafka or a socket).
#    c) Implement streaming transformations and actions to process and analyze the incoming data stream.


# In[ ]:


from pyspark.streaming import StreamingContext

# Create a StreamingContext with a batch interval of 1 second
ssc = StreamingContext(sparkContext, 1)

# Configure the application to consume data from a socket
hostname = "localhost"
port = 4041
lines = ssc.socketTextStream(hostname, port)

# Implement streaming transformations and actions
keywords = ["error", "warning", "critical"]

# Count occurrences of keywords in the streaming data
keyword_counts = lines.flatMap(lambda line: line.split(" "))                      .filter(lambda word: word.lower() in keywords)                      .map(lambda word: (word.lower(), 1))                      .reduceByKey(lambda a, b: a + b)

# Print the keyword counts
keyword_counts.pprint()

# Start the streaming computation
ssc.start()

# Wait for the streaming computation to finish
ssc.awaitTermination()


# In[ ]:


# 4. Spark SQL and Data Source Integration:
#    a) Write a Python program to connect Spark with a relational database (e.g., MySQL, PostgreSQL).
#    b)Perform SQL operations on the data stored in the database using Spark SQL.
#    c) Explore the integration capabilities of Spark with other data sources, such as Hadoop Distributed File System (HDFS) or Amazon S3.


# In[ ]:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Spark4").getOrCreate()

# Connect Spark with a relational database (e.g., MySQL, PostgreSQL)
db_url = "jdbc:postgresql://localhost:5432/mydatabase"
db_properties = {
    "user": "myuser",
    "password": "mypassword"
}

# Load data from a database table into a DataFrame
table_name = "employee"
df = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)

# Perform SQL operations on the data stored in the database
df.createOrReplaceTempView("employee1")

# Execute SQL queries on the DataFrame
result = spark.sql("SELECT * FROM employee1 WHERE empid > 10")

# Display the result
result.show()

# Explore integration with other data sources
# Read data from HDFS
hdfs_path = "hdfs://localhost:9000/data/employee.csv"
df_hdfs = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Read data from Amazon S3
s3_path = "s3a://my-bucket/data/employee.csv"
df_s3 = spark.read.csv(s3_path, header=True, inferSchema=True)

# Perform operations on the data from other data sources
# Example: Count the number of records
count_hdfs = df_hdfs.count()
count_s3 = df_s3.count()

# Display the counts
print("Count from HDFS:", count_hdfs)
print("Count from S3:", count_s3)

# Stop the SparkSession
spark.stop()

