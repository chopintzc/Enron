'''
analysis for an individual user
Created on Nov 30, 2016

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
sgraphs = {}
rgraphs = {}
communications = {}
topics = {}
snames = {}
rnames = {}
sumemails = {}
clusters = 50

user = "fossum-d"
pathpre = "D:/course/bigdata/Assign/ass3/maildir/" + user + "/output/"
pathpost = "D:/course/bigdata/Assign/ass3/completed/"
path1 = pathpost + "lda.json"
path2 = pathpost + "ldamodel.json"
path3 = pathpre + "all_s"
path4 = pathpre + "all_r"
path5 = pathpost + "words"

'''
find all the communications
'''
def findCommunications(path1, path2):
    global communications
    chdir(path1)
    files = listdir('.')
    files = [path1 + "/" + f for f in files]
    for f in files:
        if f.endswith(".json"):
            df = spark.read.json(f)
            for row in df.rdd.collect():
                c_user = row.label[:row.label.find('/')]
                c_email = row.label[row.label.rfind('/', 0, len(row.label))+1:]
                if c_user == user:
                    numbers = row.topicDistribution.values
                    min_val = 0
                    cnt = 0
                    for col in numbers:
                        if col > min_val:
                            cur_topic = cnt
                            min_val = col
                        cnt += 1
                    communications[c_email] = cur_topic
                    if not sumemails.has_key(cur_topic):
                        sumemails[cur_topic] = 0
                    sumemails[cur_topic] += 1
    save_path = path2 + "emailsummary"
    json.dump(sumemails,  open(save_path, 'w'))


'''
find all the senders
'''
def findSenders(path):
    global snames
    with open(path) as json_data:
        snames = json.load(json_data)
        json_data.close()


'''
calculate the sgraphs for each sender
'''
def calsGraphs(path):
    global snames, communications, sgraphs
    for i in communications:
        if snames.has_key(i):
            for j in snames[i]:           
                if not sgraphs.has_key(j):
                    sgraphs[j] = [0]*clusters # initialize
                sgraphs[j][communications[i]] += 1
    
    for i in sgraphs:
        max_val = max(sgraphs[i])
        cnt = 0
        for col in sgraphs[i]:
            if col == max_val:
                sgraphs[i][0] = cnt
                break
            cnt += 1
        del sgraphs[i][1:] 
    
    save_path = path[:path.rfind('/', 0, len(path))+1] + "sgraphs"
    json.dump(sgraphs,  open(save_path, 'w'))
    

'''
find all the receivers
'''
def findReceivers(path):
    global rnames
    with open(path) as json_data:
        rnames = json.load(json_data)
        json_data.close()


'''
calculate the rgraphs for each receiver
'''
def calrGraphs(path):
    global rnames, communications, rgraphs
    for i in communications:
        if rnames.has_key(i):
            for j in rnames[i]:           
                if not rgraphs.has_key(j):
                    rgraphs[j] = [0]*clusters # initialize
                rgraphs[j][communications[i]] += 1
    
    for i in rgraphs:
        max_val = max(rgraphs[i])
        cnt = 0
        for col in rgraphs[i]:
            if col == max_val:
                rgraphs[i][0] = cnt
                break
            cnt += 1
        del rgraphs[i][1:] 
    
    save_path = path[:path.rfind('/', 0, len(path))+1] + "rgraphs"
    json.dump(rgraphs,  open(save_path, 'w'))
    
    
'''
read all the words
'''
def readWords(path):
    global words
    words = [line.strip() for line in open(path, 'r')]  


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

        
findCommunications(path1, pathpre)
findSenders(path3)
calsGraphs(path3)
findReceivers(path4)
calrGraphs(path4)
#readWords(path5)
#readTopics(path5, path2)



 
