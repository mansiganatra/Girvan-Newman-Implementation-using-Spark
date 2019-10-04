from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
import sys
import json
import csv
import itertools
from time import time
import math
import random
import os
from graphframes import *

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"
)

def process(entry):
    revisedEntries= entry[0].split(',')
    return (revisedEntries[0], revisedEntries[1])

def convertValuesToTuple(entrySet):
    newEntrySet = []
    for entry in entrySet:
        newEntrySet += [(entry, 1)]
    return newEntrySet

def filter_users(user_business_map, filter_threshold):
    filtered_users = set()
    users = user_business_map.keys()

    for u1 in users:
        for u2 in users:
            if u1 != u2 and (u1, u2) not in filtered_users:
                if len(set(user_business_map.get(u1)).intersection(set(user_business_map.get(u2)))) >= filter_threshold:
                    upair = (u1, u2)
                    filtered_users.add(upair)

    return filtered_users

result = []
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
SparkContext.setSystemProperty('spark.sql.shuffle.partitions', '4')
sc = SparkContext('local[*]', 'task1')
ss = SparkSession(sc)

if len(sys.argv) != 4:
    print("Usage: ./bin/spark-submit Mansi_Ganatra_task1.py <filter_threshold> <input_file_path> <betweenness_output_file_path>")
    exit(-1)
else:
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_file_path = sys.argv[3]

# input_file_path = "C:/Users/mansi/PycharmProjects/Mansi_Ganatra_HW4/ub_sample_data.csv"
# output_file_path = "C:/Users/mansi/PycharmProjects/Mansi_Ganatra_HW4//task1__result.csv"
# filter_threshold = 7
maxIter = 5

start = time()
user_businessRdd = sc.textFile(input_file_path).map(lambda entry: entry.split('\n')).map(lambda entry: process(entry))
headers = user_businessRdd.take(1)
finalRdd = user_businessRdd.filter(lambda entry: entry[0] != headers[0][0]).persist()

user_business_map = finalRdd\
    .groupByKey()\
    .mapValues(lambda entry: list(set(entry)))\
    .collectAsMap()

filtered_user_tuples = filter_users(user_business_map, filter_threshold)
unique_users_set = set(itertools.chain.from_iterable(filtered_user_tuples))

# unique_users = [tuple([u],) for u in unique_users_set]

# unique_users_index = {}
# unique_users_rev_index = {}
#
# for key, value in enumerate(unique_users_set):
#     unique_users_index.update({key:value})
#     unique_users_rev_index.update({value:key})

unique_users = [tuple([u],) for u in unique_users_set]

user_vertices = ss.createDataFrame(unique_users, ['id'])
# user_vertices = finalRdd.map(lambda entry: entry[0]).filter(lambda entry: entry in unique_filtered_users).distinct().collect()

user_edges = sc.parallelize(filtered_user_tuples)\
    .map(lambda entry:(entry[0], entry[1], 'similar'))\
    .toDF(['src', 'dst', 'relationship'])

user_graph = GraphFrame(user_vertices, user_edges).persist()
# print(user_graph)

user_communities_df = user_graph.labelPropagation(maxIter=maxIter)
user_communities_rdd = user_communities_df.rdd\
    .map(lambda entry: (entry['label'], entry['id']))\
    .groupByKey()\
    .mapValues(lambda entry:(sorted(list(entry), key= lambda x: x[0]), len(entry)))\
    .map(lambda entry: entry[1])\
    .sortBy(lambda entry: (entry[1], entry[0]))\
    .map(lambda entry: entry[0])

user_communities = user_communities_rdd.collect()

with open(output_file_path, "w+") as fp:
    for community in user_communities:
        string_to_write = ""
        for user in community[:-1]:
            string_to_write += "\'" + user + "\', "
        string_to_write += "\'" + community[-1] + "\'"
        fp.write(string_to_write)
        fp.write("\n")

    fp.close()

end = time()
print("Duration: ", end-start)