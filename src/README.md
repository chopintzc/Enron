# **Enron**

## **Summary of the project:**

This is the Project for my Machine Learning for Big Data class, written in Python and Spark.
It can perform batch analysis on more than 100,000 Emails from ~150 email user accounts. All the emails are
from Enron Dataset. 
Two kinds of unsupervised machine learning methods (KMean and LDA) were used to perform clustering analysis
on the email dataset. All the emails are preprocessed (strip header, attachment, and quote) before clustering
analysis are performed. Each email is assigned to a cluster/topic. We can use the generated topic keywords to
summarize the content of communication embeded in the same cluster of emails
We also extract the sender and receiver information from each email. Thus for each user account, we can summarize
who is the given account communicating with and what are the prevalent email topics for the given account 

## **Requirement**

This program requires Python 2.7, Apache Spark 2.0.1, and nltk

## **Features**
* batch analysis for Enron email dataset
* data mining (strip email header, attachment, and quote)
* clustering analysis (KMean and LDA) based on the whole dataset (>100,000 emails), get a bunch of topics
* given an email account, summarize who communicated with the given account and what are the prevalent topics