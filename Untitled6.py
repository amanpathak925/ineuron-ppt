#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 1. Write a Python program to read a Hadoop configuration file and display the core components of Hadoop.


# In[ ]:


def display_hadoop_components(config_file):
    with open(config_file, 'r') as file:
        lines = file.readlines()
        
        core_components = []
        for line in lines:
            line = line.strip()
            if line.startswith('<property>'):
                component = None
            elif line.startswith('<name>'):
                component = line.split('>')[1].split('<')[0]
            elif line.startswith('<value>'):
                value = line.split('>')[1].split('<')[0]
                if component == 'fs.defaultFS':
                    core_components.append(value)
        
        print("Core Components of Hadoop:")
        for component in core_components:
            print(component)

# Specify the path to your Hadoop configuration file
config_file = 'path/to/hadoop-config.xml'

# Call the function to display the core components
display_hadoop_components(config_file)


# In[ ]:


# 2. Implement a Python function that calculates the total file size in a Hadoop Distributed File System (HDFS) directory.


# In[ ]:


import os
from hdfs import InsecureClient

def calculate_directory_size(hdfs_url, directory_path):
    client = InsecureClient(hdfs_url)

    # Function to calculate the size of a file or directory
    def get_size(path):
        if client.status(path)['type'] == 'FILE':
            return client.status(path)['length']
        else:
            total_size = 0
            for content in client.list(path):
                total_size += get_size(os.path.join(path, content['pathSuffix']))
            return total_size

    # Calculate the total size of the directory
    total_size = get_size(directory_path)
    return total_size

# Specify the HDFS URL and the directory path
hdfs_url = 'http://localhost:50070'
directory_path = '/path/to/hdfs/directory'

# Call the function to calculate the directory size
directory_size = calculate_directory_size(hdfs_url, directory_path)

print(f"Total size of directory '{directory_path}' in HDFS: {directory_size} bytes")


# In[ ]:


# 3. Create a Python program that extracts and displays the top N most frequent words from a large text file using the MapReduce approach.


# In[ ]:


import sys
from collections import defaultdict
import heapq

def mapper(text):
    # Emit key-value pairs (word, 1) for each word in the text
    words = text.split()
    for word in words:
        yield (word, 1)

def reducer(word, counts):
    # Sum up the counts for each word and emit (word, total_count)
    yield (word, sum(counts))

def top_n_frequent_words(file_path, n):
    # Initialize a dictionary to store word counts
    word_counts = defaultdict(int)

    # Open the file and process each line
    with open(file_path, 'r') as file:
        for line in file:
            # Map phase: process each line using the mapper function
            for word, count in mapper(line):
                # Aggregate the counts for each word
                word_counts[word] += count

    # Reduce phase: process the word counts using the reducer function
    reduced_counts = []
    for word, count in word_counts.items():
        for word, reduced_count in reducer(word, [count]):
            reduced_counts.append((word, reduced_count))

    # Sort the word counts in descending order
    sorted_counts = sorted(reduced_counts, key=lambda x: x[1], reverse=True)

    # Select the top N words
    top_n = heapq.nlargest(n, sorted_counts, key=lambda x: x[1])

    # Display the top N words and their counts
    for word, count in top_n:
        print(f"Word: {word}\tCount: {count}")

# Provide the file path and the value of N
file_path = 'path/to/large/text/file.txt'
N = 10

# Call the function to display the top N frequent words
top_n_frequent_words(file_path, N)


# In[ ]:


# 4. Write a Python script that checks the health status of the NameNode and DataNodes in a Hadoop cluster using Hadoop's REST API.


# In[ ]:


import requests
import json

def check_hadoop_cluster_health(nn_host):
    # Check NameNode health status
    nn_health_url = f"http://{nn_host}:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
    try:
        nn_response = requests.get(nn_health_url)
        nn_data = json.loads(nn_response.text)
        nn_status = nn_data['beans'][0]['State']
        print(f"NameNode Status: {nn_status}")
    except requests.exceptions.RequestException as e:
        print(f"Error accessing NameNode: {e}")

    # Check DataNode health status
    dn_health_url = f"http://{nn_host}:50070/jmx?qry=Hadoop:service=DataNode,name=FSDatasetState-null"
    try:
        dn_response = requests.get(dn_health_url)
        dn_data = json.loads(dn_response.text)
        dn_status = dn_data['beans'][0]['VolumeFailuresTotal']
        print(f"DataNode Status: {dn_status} volume failure(s)")
    except requests.exceptions.RequestException as e:
        print(f"Error accessing DataNodes: {e}")

# Specify the NameNode host
nn_host = 'localhost'

# Call the function to check Hadoop cluster health
check_hadoop_cluster_health(nn_host)


# In[ ]:


# 5. Develop a Python program that lists all the files and directories in a specific HDFS path.


# In[ ]:




