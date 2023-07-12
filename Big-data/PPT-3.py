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


from hdfs import InsecureClient

def list_hdfs_path(hdfs_url, hdfs_path):
    client = InsecureClient(hdfs_url)
    
    # List all the files and directories in the given HDFS path
    contents = client.list(hdfs_path, status=True)
    
    print(f"Contents of HDFS path '{hdfs_path}':")
    for content in contents:
        # Get the path and type (file or directory) of each content
        path = content['path']
        content_type = content['type']
        
        # Print the path and type
        print(f"{path}\t\t{content_type}")

# Specify the HDFS URL and the HDFS path to list
hdfs_url = 'http://localhost:50070'
hdfs_path = '/path/to/hdfs'

# Call the function to list the contents of the HDFS path
list_hdfs_path(hdfs_url, hdfs_path)


# In[ ]:


# 6. Implement a Python program that analyzes the storage utilization of DataNodes in a Hadoop cluster and identifies the nodes with the highest and lowest storage capacities.


# In[ ]:


from hdfs import InsecureClient

def analyze_storage_utilization(hdfs_url):
    client = InsecureClient(hdfs_url)
    
    # Get the DataNode information
    datanode_info = client.get_datanode_report()
    
    # Extract storage utilization information from the DataNode report
    storage_utilization = {}
    for datanode in datanode_info['beans']:
        node_name = datanode['name'].replace("DataNodeInfo", "")
        used_storage = datanode['used']
        capacity = datanode['capacity']
        storage_utilization[node_name] = (used_storage, capacity)
    
    # Identify the DataNode with the highest and lowest storage capacities
    highest_capacity_node = max(storage_utilization, key=lambda x: storage_utilization[x][1])
    lowest_capacity_node = min(storage_utilization, key=lambda x: storage_utilization[x][1])
    
    # Display the storage utilization information
    print("Storage Utilization:")
    for node, (used, capacity) in storage_utilization.items():
        utilization_percentage = (used / capacity) * 100
        print(f"{node}\tUsed: {used} bytes\tCapacity: {capacity} bytes\tUtilization: {utilization_percentage:.2f}%")
    
    print(f"\nNode with highest capacity: {highest_capacity_node}")
    print(f"Node with lowest capacity: {lowest_capacity_node}")

# Specify the HDFS URL
hdfs_url = 'http://localhost:50070'

# Call the function to analyze storage utilization
analyze_storage_utilization(hdfs_url)


# In[ ]:


# 7. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, monitor its progress, and retrieve the final output.


# In[ ]:


import requests
import json
import time

def submit_hadoop_job(yarn_url, job_conf_file):
    # Submit the Hadoop job to YARN ResourceManager
    submit_url = f"{yarn_url}/ws/v1/cluster/apps/new-application"
    try:
        response = requests.post(submit_url)
        data = json.loads(response.text)
        application_id = data['application-id']
        print(f"Application ID: {application_id}")

        # Upload the job configuration file to HDFS
        upload_url = f"{yarn_url}/ws/v1/cluster/apps/{application_id}/upload"
        with open(job_conf_file, 'rb') as file:
            upload_response = requests.post(upload_url, files={'file': file})

        # Submit the job to ResourceManager
        job_submit_url = f"{yarn_url}/ws/v1/cluster/apps"
        headers = {'Content-Type': 'application/json'}
        job_submit_payload = {
            "application-id": application_id,
            "application-name": "HadoopJob",
            "am-container-spec": {
                "commands": {
                    "command": f"hadoop jar {job_conf_file} [YOUR_JOB_ARGS]"
                }
            },
            "application-type": "MAPREDUCE"
        }
        submit_response = requests.post(job_submit_url, headers=headers, json=job_submit_payload)
        print("Hadoop job submitted successfully!")
        
        # Return the application ID
        return application_id
    except requests.exceptions.RequestException as e:
        print(f"Error submitting Hadoop job: {e}")

def monitor_job_progress(yarn_url, application_id):
    # Monitor the progress of the Hadoop job
    status_url = f"{yarn_url}/ws/v1/cluster/apps/{application_id}"
    while True:
        try:
            response = requests.get(status_url)
            data = json.loads(response.text)
            state = data['app']['state']
            final_status = data['app']['finalStatus']

            if state == 'FINISHED' and final_status == 'SUCCEEDED':
                print("Hadoop job completed successfully!")
                break
            elif state == 'FINISHED' and final_status == 'FAILED':
                print("Hadoop job failed!")
                break
            elif state == 'FINISHED' and final_status == 'KILLED':
                print("Hadoop job was killed!")
                break

            print(f"Job state: {state}\tFinal status: {final_status}")

            time.sleep(5)  # Wait for 5 seconds before checking again
        except requests.exceptions.RequestException as e:
            print(f"Error monitoring job progress: {e}")
            break

def retrieve_job_output(yarn_url, application_id):
    # Retrieve the final output of the Hadoop job
    logs_url = f"{yarn_url}/proxy/{application_id}/ws/v1/mapreduce/jobs"
    try:
        response = requests.get(logs_url)
        data = json.loads(response.text)
        job_id = data['jobs']['job'][0]['id']
        
        # Retrieve job logs
        logs_url = f"{yarn_url}/proxy/{application_id}/ws/v1/mapreduce/jobs/{job_id}/jobattempts"
        logs_response = requests.get(logs_url)
        logs_data = json.loads(logs_response.text)
        logs = logs_data['jobAttempts']['jobAttempt'][0]['logsLink']
        
        # Print the logs URL
        print(f"Job Logs: {logs}")
        
        # You can further process the logs or download the output from HDFS
        
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving job output: {e}")

# Specify the YARN ResourceManager URL and the path to the job configuration file
yarn_url = 'http://localhost:8088'
job_conf_file = '/path/to/job/conf.xml'

# Submit the Hadoop job
application_id = submit_hadoop_job(yarn_url, job_conf_file)

# Monitor job progress until completion
monitor_job_progress(yarn_url, application_id)

# Retrieve the final output of the job
retrieve_job_output(yarn_url, application_id)


# In[ ]:


# 8. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, set resource requirements, and track resource usage during job execution.


# In[ ]:


import requests
import json
import time

def submit_hadoop_job(yarn_url, job_conf_file, resource_memory, resource_vcores):
    # Submit the Hadoop job to YARN ResourceManager
    submit_url = f"{yarn_url}/ws/v1/cluster/apps/new-application"
    try:
        response = requests.post(submit_url)
        data = json.loads(response.text)
        application_id = data['application-id']
        print(f"Application ID: {application_id}")

        # Upload the job configuration file to HDFS
        upload_url = f"{yarn_url}/ws/v1/cluster/apps/{application_id}/upload"
        with open(job_conf_file, 'rb') as file:
            upload_response = requests.post(upload_url, files={'file': file})

        # Set resource requirements for the job
        resource_url = f"{yarn_url}/ws/v1/cluster/apps/{application_id}/resource"
        headers = {'Content-Type': 'application/json'}
        resource_payload = {
            "resource": {
                "memory": resource_memory,
                "vCores": resource_vcores
            }
        }
        resource_response = requests.put(resource_url, headers=headers, json=resource_payload)
        print(f"Resource requirements set: memory={resource_memory}, vCores={resource_vcores}")

        # Submit the job to ResourceManager
        job_submit_url = f"{yarn_url}/ws/v1/cluster/apps"
        headers = {'Content-Type': 'application/json'}
        job_submit_payload = {
            "application-id": application_id,
            "application-name": "HadoopJob",
            "am-container-spec": {
                "commands": {
                    "command": f"hadoop jar {job_conf_file} [YOUR_JOB_ARGS]"
                }
            },
            "application-type": "MAPREDUCE"
        }
        submit_response = requests.post(job_submit_url, headers=headers, json=job_submit_payload)
        print("Hadoop job submitted successfully!")

        # Return the application ID
        return application_id
    except requests.exceptions.RequestException as e:
        print(f"Error submitting Hadoop job: {e}")

def track_resource_usage(yarn_url, application_id):
    # Track the resource usage of the Hadoop job
    while True:
        try:
            response = requests.get(f"{yarn_url}/ws/v1/cluster/apps/{application_id}")
            data = json.loads(response.text)
            state = data['app']['state']
            final_status = data['app']['finalStatus']
            allocated_resources = data['app']['allocatedResources']
            progress = data['app']['progress']

            print(f"Job state: {state}\tFinal status: {final_status}")
            print(f"Progress: {progress}%")

            if state == 'FINISHED' and final_status == 'SUCCEEDED':
                print("Hadoop job completed successfully!")
                break
            elif state == 'FINISHED' and final_status == 'FAILED':
                print("Hadoop job failed!")
                break
            elif state == 'FINISHED' and final_status == 'KILLED':
                print("Hadoop job was killed!")
                break

            if allocated_resources is not None:
                memory = allocated_resources['memory']
                vcores = allocated_resources['vCores']
                print(f"Allocated resources: memory={memory}, vCores={vcores}")

            time.sleep(5)  # Wait for 5 seconds before checking again
        except requests.exceptions.RequestException as e:
            print(f"Error tracking resource usage: {e}")
            break

# Specify the YARN ResourceManager URL, path to the job configuration file,
# and the resource requirements for the job
yarn_url = 'http://localhost:8088'
job_conf_file = '/path/to/job/conf.xml'
resource_memory = 2048  # in MB
resource_vcores = 2

# Submit the Hadoop job
application_id = submit_hadoop_job(yarn_url, job_conf_file, resource_memory, resource_vcores)

# Track resource usage during job execution
track_resource_usage(yarn_url, application_id)


# In[ ]:


# 9. Write a Python program that compares the performance of a MapReduce job with different input split sizes, showcasing the impact on overall job execution time.


# In[ ]:


import subprocess
import time

def run_mapreduce_job(input_path, output_path, split_size):
    # Clear output directory before running the job
    subprocess.run(['hadoop', 'fs', '-rm', '-r', '-f', output_path])

    # Set the input split size
    subprocess.run(['hadoop', 'fs', '-D', 'mapreduce.input.fileinputformat.split.maxsize=' + split_size,
                    '-D', 'mapreduce.input.fileinputformat.split.minsize=' + split_size,
                    '-put', input_path, input_path])

    # Run the MapReduce job and measure execution time
    start_time = time.time()
    subprocess.run(['hadoop', 'jar', 'your_job_jar_file.jar', 'YourJobClass', input_path, output_path])
    execution_time = time.time() - start_time

    # Print execution time for the job
    print(f"Execution time with split size {split_size}: {execution_time} seconds")

# Specify the input and output paths for the MapReduce job
input_path = '/path/to/input'
output_path = '/path/to/output'

# Specify different input split sizes to compare
split_sizes = ['64', '128', '256']

# Run the MapReduce job with different split sizes and compare execution times
for split_size in split_sizes:
    run_mapreduce_job(input_path, output_path, split_size)

