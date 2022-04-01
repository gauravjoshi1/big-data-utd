# import libraries
from pyspark.mllib.linalg import Matrices, DenseMatrix
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import numpy as np

# initiate the spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('matrix-multiplication') \
    .enableHiveSupport() \
    .getOrCreate()

# Read Input Files
matrix1 = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/matrix1___small.txt")
matrix2 = sc.textFile("dbfs:/FileStore/shared_uploads/gauravjoshi0910@gmail.com/matrix2___small.txt")

# split input files into list
first_matrix = matrix1.map(lambda x : (x.split(" "))).map(lambda x : (x[:len(x) - 1]))
second_matrix = matrix2.map(lambda x : x.split(" ")).map(lambda x : (x[:len(x) - 1]))

# Turn the RDD into a list
first_list = first_matrix.collect()

# Create Indexed Row from the list 
for index in range(len(first_list)):
    first_list[index] = IndexedRow(index, [index] + first_list[index])
    
# Create an RDD from the new list
firstIndexRow = sc.parallelize(first_list)

# First Matrix Input is the first value to be multiplied
first_matrix_input = IndexedRowMatrix(firstIndexRow)

# Turn RDD into list for second matrix 
second_matrix_list = second_matrix.collect() 
length = len(second_matrix_list)

# Add first row to the second matrix
item_feature_array = [1] + [0] * len(second_matrix_list[0])

for i in range(length):
    # Add 0 to each row element as the first value
    item_feature_array += [0.0] + second_matrix_list[i]

# Build second Matrix Input using DenseMatrix 
# DenseMatrix takes 3 arguments row length, col length and input array and an optional transpose which transposes matrix
second_matrix_input = DenseMatrix(length + 1, len(second_matrix_list[0]) + 1, item_feature_array, isTransposed=True)

# Conduct Matrix Multiplication
ratings_matrix = first_matrix_input.multiply(second_matrix_input)

# extract the rating vectors
ratings_rdd = ratings_matrix.rows.map(lambda ele: (ele.index, ele.vector.toArray().tolist()))
ratings = spark.createDataFrame(ratings_rdd)
ratings.write.save('/FileStore/Output6') 