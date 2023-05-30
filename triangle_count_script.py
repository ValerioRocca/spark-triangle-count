import numpy as np
from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from collections import defaultdict
import time

def CountTriangles_prof(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def MR_ApproxTCwithNodeColors(edges, C):
  # The following lines defines the hash function hC, which assigns a color
  # between (0,C-1) to each vertex
  p = 8191
  a = np.random.randint(low=1, high=p-1)
  b = np.random.randint(low=0, high=p-1)
  def hC(u):
    return ((a * u +b) % p) % C

  # Round 1
  # The first round partitions each edge into the subsets
  E = [None] * C
  for i in range(C):
    # Filling the subsets of edges
    E[i] = edges.filter(lambda e: (hC(e[0]) == hC(e[1])) and (hC(e[0]) == i)).cache() 
        # Pyspark cache() method is used to cache the intermediate results of the transformation 
        # so that other transformation runs on top of cached will perform faster.
  
  # Computing the number of triangles
  #t = sum(map(triangle_counter, E))

  t_per_partition = []
  for i in range(C):
    t_per_partition.append(CountTriangles_prof(E[i].collect()))
  t_final = (C ** 2) * sum(t_per_partition)
  return t_final

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def MR_ExactTC(edge, C):
    p = 8191
    a = np.random.randint(low=1, high=p-1)
    b = np.random.randint(low=0, high=p-1)
    
    def h(u):
        return ((a * u +b) % p) % C
    
    # Define a function for mapping input rows to a list of tuples
    def tupler(row):
        list_final = []
        l_hashed = list(map(h, row))
        for i in range(C):
            vertex_indices = sorted((l_hashed[0], l_hashed[1], i))
            triangle_tuple = ((vertex_indices[0], vertex_indices[1], vertex_indices[2]), tuple(row))
            list_final.append(triangle_tuple)
        return list_final

    
    # Map the input RDD to a list of tuples
    triangle_tuples = edge.flatMap(tupler)
    
    # Group the tuples by key and map the groups to the number of triangles
    def group_and_to_list(x):
        return (x[0], list(x[1]))
    def count_triangles_wrapper(x):
        return countTriangles2(x[0], x[1], a, b, p, C)
    TC_1 = triangle_tuples.groupByKey() \
                                   .map(group_and_to_list) \
                                   .map(count_triangles_wrapper)

    # Reduce the triangle counts to get the total number of triangles
    TC_2 = TC_1.reduce(lambda x, y: x + y)
    
    return TC_2

if __name__ == '__main__':
    conf = SparkConf().setAppName('HW2')
    conf.set("spark.locality.wait", "0s")
    sc = SparkContext(conf=conf)
    
    # Read parameters C, R and F
    C = int(sys.argv[1])
    R = int(sys.argv[2])
    F = int(sys.argv[3]) #F=0 run approx, F=1 run exact
    
    # Read the input graph
    rawData = sc.textFile(sys.argv[4])
    # Transform into an RDD of edges, partitioned into C partitions, and cached
    edges = rawData.map(lambda x: tuple(map(int, x.split(',')))).repartition(32).cache()
    
    # Print the name of the file, C, R, and the number of edges of the graph
    print("Dataset = " + str(sys.argv[4]))
    print("Number of edges: " + str(edges.count()))
    print("Number of Colors: " + str(C))
    print("Number of Repetitions:" + str(R))

    if F==0:
        
        # Run MR_ApproxTCwithNodeColors R times to get R independent estimates t_final of the number of triangles in the input graph
        t_final_values = []
        start_time = time.time()
        for i in range(R):
            t_final_values.append(MR_ApproxTCwithNodeColors(edges, C))
        end_time = time.time()

        # Print the median of the R estimates returned by MR_ApproxTCwithNodeColors and the average running time of MR_ApproxTCwithNodeColors over the R runs
        print('Approximation algorithm with node coloring')
        print("- Number of triangles (median over " + str(R) + "runs)= " + str(np.median(t_final_values)))
        print("- Running time (average over " + str(R) + "runs) = " + str((end_time - start_time)/R*1000) + "ms")

    elif F==1:
        
        # Run exact_triangles R times to get R independent estimates t_final of the number of triangles in the input graph
        t_final_values = []
        start_time = time.time()
        for i in range(R):
            t_final_values.append(MR_ExactTC(edges, C)) #exact_triangles
        end_time = time.time()

        # Print the median of the R estimates returned by exact_triangles and the average running time of exact_triangles over the R runs
        print('Exact algorithm with node coloring')
        print("- Number of triangles = "+ str(np.median(t_final_values)))
        print("- Running time (average over " + str(R) + "runs)= " + str((end_time - start_time)/R*1000) + "ms")

    else:
        print("ERROR: F has to be either 0 or 1")
        print("- 0 to perform approximate TC")
        print("- 1 to perform exact TC")
    
    # Stop the SparkContext
    sc.stop()