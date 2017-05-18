'''
Kmean clustering analysis
Created on Nov 27, 2016

@author: Zhongchao
'''

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from os import listdir, chdir
import os
import time
import csv, sys

'''
get_immediate_subdirectories
'''
def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]
    
'''
hasfolder
'''   
def hasfolder(a_dir):
    d = get_immediate_subdirectories(a_dir)
    flag = False
    for curd in d:
        if curd == "all_documents":
            flag = True
            break
    return flag


spark = SparkSession\
    .builder\
    .appName("PythonKMeansExample")\
    .getOrCreate()


chdir("D:/course/bigdata/Assign/ass3/maildir/")

'''
parameters
'''
NumofFeatures = 1500
KValue = 10
SValue = 1
printLines = 5
user = "beck-s"
path = "D:/course/bigdata/Assign/ass3/maildir"
all_data = []
csv.field_size_limit(sys.maxint)


'''
traverse all the subdirectories
'''
d = get_immediate_subdirectories(path)

for curd in d:
    subpath = path + "/" + curd
    if hasfolder(subpath):
        with open(path + "/" + curd + "/" + "output" + "/" + "processedemails.csv", 'rb') as f:
            reader = csv.reader(f)
            for rec in csv.reader(f, delimiter=','):
                str1 = curd + "/" + rec[0] 
                str2 = rec[1]
                tuple_element = (str1, str2)
                all_data.append(tuple_element)

'''
read all the datas
'''
'''
chdir(path)
files = listdir('.')
files = [path + "/" + f for f in files]
cnt = 0
for f in files:
    line = open(f, 'r').read()
    element_tuple = str_tuple = (cnt, line)
    all_data.append(element_tuple)
    cnt += 1
'''

'''
Tokenize all the words
'''
sentenceData = spark.createDataFrame(all_data, ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
print "original words:"
for words_label in wordsData.select("words", "label").take(printLines):
    print(words_label)

    
'''
calculate temporal frequency
'''
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=NumofFeatures)
featurizedData = hashingTF.transform(wordsData)
#cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=200, minDF=2.0).fit(wordsData)
#featurizedData = cv.transform(wordsData)
print "words after TF:"
#print cv.vocabulary
for words_label in featurizedData.select("words", "label").take(printLines):
    print(words_label)


'''
calculate IDF
'''
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
dataset = idfModel.transform(featurizedData)
print "words after IDF:"
for features_label in dataset.select("features", "label").take(printLines):
    print(features_label)


'''
KMean analysis
''' 
start_time = time.time()
kmeans = KMeans().setK(KValue).setSeed(SValue)
model = kmeans.fit(dataset)
print("Executation time:  %s seconds ---" % (time.time() - start_time))

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
