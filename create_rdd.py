from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
""" Create RDD"""
# df = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'),
#                                      (4, 5, 6, 'd e f'),
#                                      (7, 8, 9, 'g h i')]).toDF(['col1', 'col2', 'col3', 'col4'])

# df.show()
# print(df.collect())
"""Create Data Frame"""
# Employee = spark.createDataFrame([
#     ('1', 'Joe', '70000', '1'),
#     ('2', 'Henry', '80000', '2'),
#     ('3', 'Sam', '60000', '2'),
#     ('4', 'Max', '90000', '1')],
#     ['Id', 'Name', 'Salary', 'DepartmentId']
# )
#
# Employee.show()
"""Create DataFrame"""
my_list = [['a', 1, 2], ['b', 2, 3], ['c', 3, 4]]
col_name = ['A', 'B', 'C']

# Using Pandas
# print(pd.DataFrame(my_list, columns=col_name))
# Attention: Pay attentation to the parameter columns= in pd.DataFrame. Since the default value
# will make the list as rows.
# print(pd.DataFrame(my_list, col_name))
# Using Spark
# spark.createDataFrame(my_list, col_name).show()

# From Dictionary
# d = {'A': [0, 1, 0],
#      'B': [1, 0, 1],
#      'C': [1, 0, 0]}

# print(pd.DataFrame(d))
# spark.createDataFrame(np.array(list(d.values())).T.tolist(),list(d.keys())).show()
"""Read CSV File"""
dp = pd.read_csv("/home/aman/Downloads/SampleCSVFile_2kb.csv", encoding='latin1')
# print(dp)
ds = spark.read.csv(path='/home/aman/Downloads/SampleCSVFile_2kb.csv',
                    # sep=',',
                    # encoding='UTF-8',
                    # comment=None,
                    header=True,
                    inferSchema=True)
# df = spark.read.format("csv").load("/home/aman/Downloads/SampleCSVFile_2kb.csv")

# df = spark.read.format("org.apache.spark.sql.csv").load("/home/aman/Downloads/SampleCSVFile_2kb.csv")
# df.printSchema()
# ds.printSchema()
print(ds[['id','Nam']].show(4))
print(dp[['id','Nam']].head(4))
