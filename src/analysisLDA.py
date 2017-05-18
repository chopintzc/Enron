'''
LDA clustering analyis
Created on Nov 28, 2016

@author: Zhongchao
'''

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SparkSession
from pyspark.ml.clustering import LDA
from os import listdir, chdir
from pyspark.ml.feature import CountVectorizer
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
    .appName("TokenizerExample")\
    .getOrCreate()


chdir("D:/course/bigdata/Assign/ass3/maildir/")

'''
parameters
'''
vocabNumber = 1500
minDFValue = 2.0
KValue = 20
iterNumber = 20
topicsNumber = 20
printLines = 5
user = "allen-p"
path = "D:/course/bigdata/Assign/ass3/maildir"
savepath = "D:/course/bigdata/Assign/ass3/completed"
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
files = listdir('.')
files = [path + "/" + f for f in files]
all_data = []
for f in files:
    cnt = f[f.rfind('/', 0, len(f))+1:]
    line = open(f, 'r').read()
    element_tuple = (cnt, line)
    all_data.append(element_tuple)
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
#hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
#featurizedData = hashingTF.transform(wordsData)
cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=vocabNumber, minDF=minDFValue).fit(wordsData)
featurizedData = cv.transform(wordsData)
print "words after TF:"
print cv.vocabulary
for words_label in featurizedData.select("rawFeatures", "words").take(printLines):
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
LDA analysis
'''    
start_time = time.time()
lda = LDA(k=KValue, maxIter=iterNumber)
model = lda.fit(dataset)
ll = model.logLikelihood(dataset)
lp = model.logPerplexity(dataset)
print("Lower bound on the log likelihood: " + str(ll))
print("Upper bound on perplexity: " + str(lp))
print("Executation time:  %s seconds ---" % (time.time() - start_time))
# Describe topics.
topics = model.describeTopics(topicsNumber)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)
# Shows the result
transformed = model.transform(dataset)
transformed.show(truncate=False)


'''
write down lda and ldamodel to json format
'''
topics.write.save(os.path.join(savepath + "/ldamodel.json"), format="json")
transformed.write.save(os.path.join(savepath + "/lda.json"), format="json")

thefile = open(os.path.join(savepath + "/words"), 'w')
for item in cv.vocabulary:
    thefile.write("%s\n" % item)
  
spark.stop()