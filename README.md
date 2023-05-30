# PySpark Triangle Count
This repository contains the code for my homework project in Big Data Computing. The objective was to estimate the number of triangles in an undirected graph using two PySpark algorithms. The algorithms were then tested on the Orkut dataset using CloudVeneto's cluster.

###Files
Here is a summary of the files contained in the repo.
- *triangle_count_script*. A Python 3.6 script containing the two algorithms.
- *triangle_count_results*. A PDF file containing the main results.
- *facebook_small*. A small undirected graph on which you can test the algorithm.

###Algorithms
The program implements the following two color-based 2-round MapReduce algorithms.
- MR_ApproxTCwithNodeColors: approximate estimate.
- MR_ExactTC: exact estimate.

###Orkut Dataset
The file Orkut117M represents the Orkut social network and it has 177,185,083 edges and 3,072,441 nodes. The files OrkutXM with X in {1,2,4,8,16,32,64} are subsets of the previous with X millions edges. More details here: https://snap.stanford.edu/data/com-Orkut.html)


