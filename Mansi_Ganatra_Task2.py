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

def process(entry):
    revisedEntries= entry[0].split(',')
    return (revisedEntries[0], revisedEntries[1])

def convertValuesToTuple(entrySet):
    newEntrySet = []
    for entry in entrySet:
        newEntrySet += [(entry, 1)]
    return newEntrySet


def generate_community_users(user_business_map, filter_threshold):
    nearby_users_map = {}
    users = user_business_map.keys()

    for u1 in users:
        related_users = set()
        for u2 in users:
            if u1 != u2:
                u1_businesses = set(user_business_map.get(u1))
                u2_businesses = set(user_business_map.get(u2))
                common_businesses = u1_businesses.intersection(u2_businesses)

                if len(common_businesses) >= filter_threshold:
                    # non_common_businesses = set(u1_businesses.difference(u2_businesses)).union(set(u2_businesses.difference(u1_businesses)))
                    related_users.add(u2)
        if len(related_users) > 0:
            nearby_users_map.update({u1:related_users})

    return nearby_users_map

def generate_betweenness_map(root, nearby_users_map):

    # final result
    edges_betweenness_map = {}

    # map for all nodes at each level
    level_nodes_map = {}

    # map for all child->{parents} to calculate partial credit in case of multiple shortest paths
    child_parents_map = {}
    current_level = 0

    current_child_nodes = set([root])
    level_nodes_map.update({root:current_level})

    # start traversing in bfs - steps 1 & 2 of Girvan-Newman

    while len(current_child_nodes) > 0:

        current_level += 1

        # children for next iteration
        children_for_next_iter = set()

        for child in current_child_nodes:
            # print("Current child: ", child)
            current_grandchildren = nearby_users_map.get(child)

            # print("Current grandchildren: ", current_grandchildren)

            # because we need al the nodes at next level and one node can be reachable through multiple parents
            # children_for_next_iter = children_for_next_iter.union(current_grandchildren)

            for grandchild in current_grandchildren:
                # encountering this node for the first time
                if not level_nodes_map.__contains__(grandchild):
                    level_nodes_map.update({grandchild:current_level})
                    child_parents_map.update({grandchild:set([child])})
                    children_for_next_iter.add(grandchild)
                # condition required to eliminate siblings to not include them in parent map
                # as original map contains all neighbours not just parents
                # This stage won't be reached until the node is once added to the map by it's parent
                elif level_nodes_map.get(grandchild) == level_nodes_map.get(child)+1:
                    # print("Current Parents: ", child_parents_map.get(grandchild))
                    child_parents_map.get(grandchild).add(child)
                    # print("Updated_Parents: ", child_parents_map.get(grandchild))
                    # child_parents_map.update({grandchild:new_parents})

        current_child_nodes = children_for_next_iter.difference(set([root]))
        # print("Nodes for next iteration: ", current_child_nodes)

    # traversal completed
    # Step 3 of Girvan-Newman

    # all nodes not occurring as parents in child_parents_map are leaf nodes

    # flatten all values and get unique
    # unique_parents = set([parent for parent_group in child_parents_map.values() for parent in parent_group])

    # all nodes except root - as root is not assigned any value
    all_nodes = set(level_nodes_map.keys())#.difference(set([root]))

    # leaf_nodes = all_nodes.difference(unique_parents)

    # calculate partial credits for each node starting from leaf node, each node has at least 1 credit other than root
    node_partial_credits_map = {}


    # Each leaf node in the DAG gets a credit of 1
    for node in all_nodes:
        node_partial_credits_map.update({node:1.0})

    # print(current_level)

    # current_level is one before the last level
    while current_level > 0:
        current_level_nodes = [k for k,v in level_nodes_map.items() if v == current_level]

        # Each node that is not a leaf gets credit = 1 + sum of credits of the DAG edges from that node to level below
        for node in current_level_nodes:

            # Equally divide the current node's credit for all its parents
            partial_credit = float(node_partial_credits_map.get(node))/len(child_parents_map.get(node))

            parents_of_current_node = child_parents_map.get(node)

            for parent in parents_of_current_node:
                # Add current nodes share of credit to all its parents
                updated_credit = node_partial_credits_map.get(parent) + partial_credit
                node_partial_credits_map.update({parent:updated_credit})

                # build betweenness for all edges going out from current node to it's parents,
                # as we have their total weight calculated at this point because of bottom-up approach
                edges_betweenness_map.update({(node, parent):partial_credit})

        current_level -= 1

    return edges_betweenness_map

def generate_betweenness_result(nearby_users_map):
    all_nodes = nearby_users_map.keys()
    users_betweenness_map = {}

    final_result = set()

    for root in all_nodes:
        edges_betweenness_map = generate_betweenness_map(root, nearby_users_map)
        for edge, betweennness in edges_betweenness_map.items():
            if edge in users_betweenness_map.keys():
                updated_betweenness = users_betweenness_map.get(edge)+ betweennness
                users_betweenness_map.update({edge:updated_betweenness})
            else:
                users_betweenness_map.update({edge:betweennness})

    for edge, betweennness in users_betweenness_map.items():
        final_result.add((tuple(sorted(list(edge))),float(betweennness)/2))
        # final_result.append((edge, float(betweennness)/2))
        # users_betweenness_map.update({edge:float(betweennness)/2})

    final_users_betweenness_map = sc.parallelize(final_result)\
            .sortBy(lambda entry: (-entry[1], entry[0][0]))\
            .collectAsMap()

    return final_users_betweenness_map

def generate_adjacency_matrix(nearby_users_map):

    users = nearby_users_map.keys()
    adjacency_matrix = {}
    for u1 in users:
        for u2 in users:
            if set(nearby_users_map.get(u1)).__contains__(u2):
                adjacency_matrix.update({tuple(sorted([u1,u2])):1.0})
            else:
                adjacency_matrix.update({tuple(sorted([u1, u2])): 0.0})

    return adjacency_matrix

def generate_degree_matrix(nearby_users_map):

    users =  nearby_users_map.keys()
    degree_matrix = {}

    for u1 in users:
        degree_matrix.update({u1:len(nearby_users_map.get(u1))})

    return degree_matrix

def generate_user_clusters(graph, vertices):
    adjacent_users_map_copy = graph.copy()
    clusters = set()

    unique_users = set(graph.keys())

    for user in unique_users:
        current_members = set()
        current_cluster = set()

        if user in adjacent_users_map_copy.keys():
            current_members = adjacent_users_map_copy.get(user)
            current_cluster = set([user])

        # current_cluster = set()

        while len(current_members) > 0:
            members_for_next_iteration = set()
            for current_member in current_members:
                current_cluster.add(current_member)
                if current_member in adjacent_users_map_copy.keys():
                    members_for_next_iteration = members_for_next_iteration.union(set(adjacent_users_map_copy.get(current_member)))

                    adjacent_users_map_copy.pop(current_member)

            current_members = members_for_next_iteration.difference(set([user]))
        if len(current_cluster) > 0:
            clusters.add(tuple(sorted(list(current_cluster))))

    return clusters


def run_girvan_newman(graph, adjacency_matrix, degree_matrix, m, users_betweenness_map, nearby_users_map, vertices):

    number_of_clusters = len(graph)
    clusters = graph

    # calculate Modularity Q based on formula:
    current_q = 0
    # previous_q = 0

    # available_edges = list(users_betweenness_map.keys())
    # num_available_edges = len(available_edges)
    max_q = float('-inf')
    max_clusters = {}

    # index = 0
    while users_betweenness_map:
        # previous_q = current_q

        # we updated the clusters after the iteration or first time, we need to calculate Q
        # if number_of_clusters != len(clusters) or index == 0:
        # number_of_clusters = len(clusters)
        total_minus_expected = 0

        for cluster in clusters:
            cl = list(cluster)
            for u1 in cl:
                for u2 in cl:
                    if u1 < u2:
                        aij = adjacency_matrix.get((u1, u2))
                        ki = degree_matrix.get(u1)
                        kj = degree_matrix.get(u2)
                        mod_sum = aij - (float(ki * kj) / (2 * m))
                        total_minus_expected += mod_sum

        current_q = float(total_minus_expected) / (2 * m)
        if current_q > max_q:
            max_q = current_q
            max_clusters = clusters

            # if index == 0:
            #     previous_q = current_q

        # calculation for q completed

        # get the edge with maximum betweenness to be dropped to divide into communities

        edges_to_drop = []
        max_betweenness = max(users_betweenness_map.values())
        for edge_to_drop in users_betweenness_map.keys():
            if users_betweenness_map.get(edge_to_drop) == max_betweenness:
                edges_to_drop.append(edge_to_drop)
                # edge_to_drop_betweenness = users_betweenness_map.get(edge_to_drop)

                # update all the matrices to remove this edge
                # adjacency_matrix.pop(edge_to_drop)
                if degree_matrix.get(edge_to_drop[0]) > 0:
                    degree_matrix.update({edge_to_drop[0]: (degree_matrix.get(edge_to_drop[0]) - 1)})
                if degree_matrix.get(edge_to_drop[1]) > 0:
                    degree_matrix.update({edge_to_drop[1]: (degree_matrix.get(edge_to_drop[1]) - 1)})

                # remove users from each others connected list

                updated_u1 = set(nearby_users_map.get(edge_to_drop[0])).difference(set([edge_to_drop[1]]))
                # print("Original node1 length: ", len(nearby_users_map.get(edge_to_drop[0])))
                nearby_users_map.update({edge_to_drop[0]: updated_u1})
                # print("Updated node1 length: ",  len(nearby_users_map.get(edge_to_drop[0])))
                # print("Original node2 length: ", len(nearby_users_map.get(edge_to_drop[1])))
                updated_u2 = set(nearby_users_map.get(edge_to_drop[1])).difference(set([edge_to_drop[0]]))
                nearby_users_map.update({edge_to_drop[1]: updated_u2})
                # print("Updated node2 length: ", len(nearby_users_map.get(edge_to_drop[1])))

        for edge in edges_to_drop:
            del users_betweenness_map[edge]

        # users_betweenness_map = generate_betweenness_result(nearby_users_map)

        # print("Previous num of clusters: " , number_of_clusters)
        clusters = generate_user_clusters(nearby_users_map, vertices)
        # print("Current number of clusters: ", len(clusters))
        # m = len(users_betweenness_map.keys())
        # index += 1
    return max_clusters

if len(sys.argv) != 5:
    print("Usage: spark-submit	firstname_lastname_task2.py	 <filter_threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>")
    exit(-1)
else:
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    betweenness_output_file_path = sys.argv[3]
    community_output_file_path = sys.argv[4]


result = []
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
SparkContext.setSystemProperty('spark.sql.shuffle.partitions', '4')
sc = SparkContext('local[*]', 'task2')
ss = SparkSession(sc)

# input_file_path = "C:/Users/mansi/PycharmProjects/Mansi_Ganatra_HW4/ub_sample_data.csv"
# betweenness_output_file_path = "C:/Users/mansi/PycharmProjects/Mansi_Ganatra_HW4//task2__result_1.csv"
# community_output_file_path = "C:/Users/mansi/PycharmProjects/Mansi_Ganatra_HW4//task2__result_2.csv"
# filter_threshold = 7

start = time()
user_businessRdd = sc.textFile(input_file_path).map(lambda entry: entry.split('\n')).map(lambda entry: process(entry))
headers = user_businessRdd.take(1)
finalRdd = user_businessRdd.filter(lambda entry: entry[0] != headers[0][0]).persist()

user_business_map = finalRdd\
    .groupByKey()\
    .mapValues(lambda entry: list(set(entry)))\
    .collectAsMap()

# ############################## Betweenness Calculation ############################
nearby_users_map = generate_community_users(user_business_map, filter_threshold)
# print(nearby_users_map)
users_betweenness_map = generate_betweenness_result(nearby_users_map)

with open(betweenness_output_file_path, "w+") as fp:
    for edge, betweenness in users_betweenness_map.items():
        string_to_write = "(\'" + edge[0] + "\', \'" + edge[1] +"\'), " + str(betweenness)
        fp.write(string_to_write)
        fp.write("\n")

# ################################# Community Detection #############################################

# nearby_users_map = generate_community_users(user_business_map, filter_threshold)

adjacency_matrix = generate_adjacency_matrix(nearby_users_map)

degree_matrix = generate_degree_matrix(nearby_users_map)

m = len(users_betweenness_map.keys())

# split network into subgraphs by removing edge with highest betweenness

clusters = generate_user_clusters(nearby_users_map, nearby_users_map.keys())

optimized_clusters = run_girvan_newman(clusters, adjacency_matrix, degree_matrix, m, users_betweenness_map, nearby_users_map, nearby_users_map.keys())

user_communities_rdd = sc.parallelize(optimized_clusters)\
    .map(lambda entry:(sorted(list(entry), key= lambda x: x[0]), len(entry)))\
    .sortBy(lambda entry: (entry[1], entry[0]))\
    .map(lambda entry: entry[0])
#
user_communities = user_communities_rdd.collect()

with open(community_output_file_path, "w+") as fp:
    for community in user_communities:
        string_to_write = ""
        for user in community[:-1]:
            string_to_write += "\'" + user + "\', "
        string_to_write += "\'" + community[-1] + "\'"
        fp.write(string_to_write)
        fp.write("\n")

    fp.close()
# print(optimized_clusters)
end = time()
print("Duration: ", end-start)