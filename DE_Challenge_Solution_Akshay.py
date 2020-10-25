# Databricks notebook source
from pyspark import SparkContext
import string

#Setting spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
print("\n\n\n")

#Change path of input dataset and the out directory according to system. It currently contains databricks file system paths.
inpath = "dbfs:/FileStore/glofox/dataset/"
outpath = "dbfs:/FileStore/glofox/"

#Create RDD and load dataset. sc.wholeTextFiles returns a PairRDD with the key being the file path and the value being the content of the document.
inputrdd = sc.wholeTextFiles(inpath)


#### RDD1 - Read docs, extract words and doc_ids (Word, [docid1, docid2....docidn]) ####
#Flatmap to combine all the document content for all files, split into words and form tuples with (word, [doc_id]). Only some basic text cleaning (fast punctuations removal) has been done while parsing documents as this is not the focus. ReduceByKey transformation to group keys (word) and append values (document_ids) to list. set() function is used within ReduceByKey and sorted() in a subsequent map to improve efficiency (More details in Readme.md)
parse_distinct_words_rdd = inputrdd.flatMap(lambda filewordpair :((word,[int(filewordpair[0].replace(inpath,""))]) for word in filewordpair[1].translate(str.maketrans('', '', string.punctuation)).lower().split()))\
      .reduceByKey(lambda a,b: list(set(a+b)))\
      .map(lambda sortlist: (sortlist[0], sorted(sortlist[1])))

#.reduceByKey(lambda a,b: sorted(set(a+b)))

#Spark provides an optimization mechanism to store the intermediate computation of an RDD so they can be reused in subsequent actions. When you persist or cache an RDD, each worker node stores it’s partitioned data in memory or disk and reuses them in other actions on that RDD.
parse_distinct_words_rdd.cache()


#### RDD2 - Dictionary (word , word_id) ####
#RDD.zipWithIndex adds contiguous (Long) numbers. This needs to count the elements in each partition first, so the input will be evaluated twice. So, we have cached parse_distinct_words_rdd.
wordid_dict_rdd = parse_distinct_words_rdd.map(lambda word : word[0]).zipWithIndex()  

#Writing the  dictionnary (word , word_id) to text file . Every line of the output file will be the word and its id.
wordid_dict_rdd.saveAsTextFile(outpath+"word_dictionary_final")


#### Join RDD1 and RDD2 and keep only necessary columns to obtain final inverted index (word_id, [docid1, docid2....docidn]) ####
#Join transformation to join the two rdds ((word, word_id) and (word, [doc_id1,doc_id2...doc_idn])) on words to obtain the (word_id,[doc_id1, doc_id2...doc_idn]) pair and sort by word_ids.
inverted_index_rdd =  parse_distinct_words_rdd.join(wordid_dict_rdd)\
    .map(lambda result : (result[1][1],result[1][0]))\
    .sortBy(lambda fileid : fileid[0])
# Unit test : Check if correct mapping of word -> word_id was obtained in Inverted index - Compare data in rdds wordid_dict_rdd and inverted_index_rdd (inverted_index_rdd contains (word, word_id, [doc_ids]) ) to see if the mapping of word -> word_id is consistent in both rdds.

#Writing the final inverted index to a text file. Every line of output file contains word_id and the list of doc_ids it appears in (word_id, [doc_id1, doc_id2....doc_idn]). Both the word ids and the doc_ids in the corresponding lists are sorted.
inverted_index_rdd.saveAsTextFile(outpath+"inverted_index_final")


#prints to test and debug sample output data
#print(wordid_dict_rdd.take(60))
#print()
#print(inverted_index_rdd.take(60))


"""

Finally use below command on Hadoop fs to merge the output partition files into single file.

hadoop fs -getmerge -nl /src/part0000 /src/part0001 /output.txt

copymerge() call through Hadoop API through Python has been discontinued and hence we can perform this externally by hadoop fs getmerge.

"""
