'''
generate a summary report for the dataset
Created on Nov 29, 2016

@author: Zhongchao
'''

from pyspark.sql import SparkSession
from os import listdir, chdir
import json
import os


    
spark = SparkSession\
    .builder\
    .appName("analysisAPI")\
    .getOrCreate()
    

'''
parameters
'''
communications = {}
topics = {}
clusters = 20


pathpre = "D:/course/bigdata/Assign/ass3/completed/"
path1 = pathpre + "lda.json"
path2 = pathpre + "ldamodel.json"
path3 = pathpre + "all_s"
path4 = pathpre + "all_r"
path5 = pathpre + "words"


'''
find all the communications
'''
def findCommunications(path):
    global communications
    chdir(path)
    files = listdir('.')
    files = [path1 + "/" + f for f in files]
    for f in files:
        if f.endswith(".json"):
            df = spark.read.json(f)
            for row in df.rdd.collect():
                numbers = row.topicDistribution.values
                min_val = 0
                cnt = 0
                for col in numbers:
                    if col > min_val:
                        cur_topic = cnt
                        min_val = col
                    cnt += 1
                if not communications.has_key(cur_topic):
                    communications[cur_topic] = 0
                communications[cur_topic] += 1
    save_path = path[:path.rfind('/', 0, len(path))+1] + "emailsummary"
    json.dump(communications,  open(save_path, 'w'))

'''
read all the topics
'''
def readTopics(path1, path2):
    global topics
    chdir(path2)
    files = listdir('.')
    files = [path2 + "/" + f for f in files]
    for f in files:
        if f.endswith(".json"):
            df = spark.read.json(f)
            for row in df.rdd.collect():
                topic = row.topic
                numbers = row.termIndices
                cur_topic = [words[i] for i in numbers]
                topics[topic] = cur_topic

    save_path = path1[:path1.rfind('/', 0, len(path1))+1] + "topics"
    json.dump(topics,  open(save_path, 'w'))
    
def readWords(path):
    global words
    words = [line.strip() for line in open(path, 'r')]


readWords(path5)  
readTopics(path5, path2)   
findCommunications(path1)
