# Data engineering coding challenge

This is a technichal test for data engineering candidates at Glofox, this test was [originally designed by D2SI](https://github.com/d2si-oss/data-engineering-coding-challenge).

### Terms

Candidates should do this test on their own. It is designed to be done in 3 or 4 hours but there is no hard limit. Please upload your solution to any VCS like Github, Bitbucket, Gitlab, etc. and let us know the link to your repo.

Please make sure to include a README and any information you might think it's relevant of your project, including features that you'd like to implement if you feel that you were time-constrained.


# Solution


## Execution Instructions
The solution was developed using pyspark, using python programming language and databricks was the cloud based ide chosen that supports distributed applications and provides good distributed computing resources. 
The solution can be executed either on Databricks or on a local machine having spark, latest version of python and Java SDE 8 or above installed on it.

To reproduce the results as with the existing code using Databricks, 
1. Create a community edition account.
2. Upload the input dataset to the paths using Upload File option in the dropdown option "File" in the ide.
3. Create and attach cluster with default resources provided by Databricks Community Edition.
4. Run the code.
5. Download the output partition files using Databricks cli (Need pip install databricks-cli).
6. Merge the output partition files on Hadoop fs using getmerge command.

You can run it locally on a machine having spark after changing the path of input dataset and the out directory according to system. It currently contains databricks file system paths. (Databricks is a platform that runs on top of Apache Spark, so it should not be a problem).
Then merge the output partition files on Hadoop fs using getmerge command.

## Approach Taken

The ulterior goal of the project is that the solution should work on a massive dataset.
There are several ways to tackle this problem, the best way using distributed programming, that runs on a distributed system so that we are not limited by the amount of storage, memory and CPU of a single machine.
While blocked sort-based indexing that is mentioned in the challenge description is a potential solution, manually dividing the data into blocks, processing and then merging by code would be not only increase the tediousness but also may disturb the readability and concision of the code.
Due to these reasons, the solution was implemented adopting Spark as the medium for using distributed computing.
The solution was developed using pyspark, using python programming language and databricks was the cloud based ide chosen that supports distributed applications and provides good distributed computing resources. 
This is a Apache-Spark based platform which runs a distributed system behind the scenes, so that the workload is automatically split across various processors which scale up and down on demand. This results in increased efficiency and direct time and cost savings for massive tasks. 

After deciding on all the technical solution design considerations, code development was started using Agile methodology.
In the first sprint, only the basic functionality was developed. Only after the basic functionality was achieved, the next sprint involved removal of redundant and debug level purpose code. This also involved tailoring the code to achieve any improvements in performance, efficiency and execution speed.
Development was done using a part of the dataset. However, the final code was run and thoroughly tested using the complete dataset. The testing phase involved unit tests after adding features during the development phase as well as the functional and data testing performed at the end.

## Architecture

### Solution Architecture

The solution was developed using pyspark, using python programming language and databricks was the cloud based ide chosen that supports distributed applications and provides good distributed computing resources. 


### Functional Architecture

The Architecture describes the tasks and subtasks to achieve the 4 main algorithm steps

1. Read the documents and collect every pair (wordID, docID)
2. Sort those pairs by wordID and by docID
3. For every wordID, group the pairs so you have its list of documents
4. Merge the intermediate results to get the final inverted index

#### Read and collect words -> (wordID, docID) -> Sort -> Group and reduce -> Merge

Based on this, the functional architecture that was proposed was as below. Since we are using partitions and Reducing by Key uses repartition operations, it makes sense to Sort at a later stage than Reduce (and after join for sorting word_ids). 

#### Read docs,collect words and doc_ids (Word, [Doc_ids]) -> (Word, word_id) Dictionary and output file -> Join to obtain (word_id, [Doc_ids]) , sort and write output file -> Merge the output file partitions externally (Hadoop)

#### RDD1 -> RDD2 & write -> JOIN & Write -> Merge written result partitions


### Components of Functional Architecture

1. RDD1 :  Read docs,collect words and doc_ids (Word, [Doc_ids]) 

#### Parse docs -> Collect words -> Group and Reduce by words to obtain corresponding doc_ids -> Sort list of doc_ids


2. RDD2 : (Word, word_id) Dictionary and output file 

#### Cache RDD of Component 1 -> Zip with index to (Word, word_id) -> Write results


3. Join to obtain (word_id, [Doc_ids]) , sort and write output file

#### Join RDD1 and RDD2 -> Extract only necessary columns (word_id, [Doc_ids]) -> Sort by word_id -> Write results


4. Merge the output file partitions externally (Hadoop)

## hadoop fs getmerge to merge partition files into single file



## Code structure and clarity

The program is designed to achieve code concision, readability and clarity. It was made sure to remove any code redundancy. All code is commented for meaningful description of subtasks performed.
The code is tailored to achieve high performance and give correct results.


## Performance

Efforts have been made in the code to achieve optimal performance.
There are 2 saveAsTextfile actions (1 for word-word_id dictionary and other for inverted index) ,1 action is associated with reducebykey.
Although RDD1 has been cached, there is also zipwithindex() function which needs to count elements in each partition and zip it with ids.
Finally RDD1 and Rdd2 are joined which results in repartition. It is followed by sortBy to sort the inverted index according to word_ids.

These result in about 5 spark jobs and the average execution time was recorded around 28.5 seconds.


Some performance improvement optimizations considered-

1. Sorting in ReduceByKey vs using ReduceByKey and then sorting in a subsequential map 

Sorting in ReducebyKey may be more expensive as it performs sorting in every reduce operation.
If you assume the best case scenario for Timsort (runs in O(N) for best case) the first approach is N times O(N) as it runs for every reduce operation. The second one to sort at the end will give O(N log N) in the worst case scenario.

We can compare some sample run times noticed for both approaches.
Approach 1 execution run times (in secs) : 29.52, 29.98, 29.64, 29.11.
Approach 2 execution run times (in secs): 28.18, 28.11, 28.44, 28.54.


2. Removing duplicates before sorting 

In this case as there will be huge number of duplicates for the file ids in which words appear(1 word may appear many times in a file resulting in duplicates while using reduceby), set() was used to remove duplicates  before sorting so that sorting becomes less expensive as the size of list to be sorted is greatly decreased.


3. RDD1 Cache

It makes sense to cache RDD1 (word, [Doc_ids]) as we are reusing this RDD later in constructing RDD2 and during join operation.
Also, RDD.zipWithIndex is used in RDD2 (word, word_id) which is built on RDD1. This needs to count the elements in each partition first, so your input will be evaluated twice. So it makes sense to cache RDD1.


## Testing

The testing phase involved unit tests after adding features during the development phase as well as the functional and data testing performed at the end.


#### Data Quality Check

1. Check for data quality in input dataset. Quick analysis on type of data. Contains special symbols, non- ASCII standard characters, words seperated by multiple spaces.


#### Data Transformation logic checks

1. Word_ids and corresponding list of doc_ids are generated both in sorted order - Simple count check in excel.
2. Sample records tested to check if word is actually appearing in all the documents of the corresponding doc_ids in (word, [doc_ids]) - grep/findstr recursively in linux/windows.
	Eg. Find word "pests" in all docs (findstr /r  /S /I /M /C:"\<pests\>" *.* ). Gives result 3,15 which matches correctly with our solution.
	
- Observation and discrepancies found: Some documents like Document 9 contains special symbols between words (eg. right--so). Current cleaning process removes the special symbols and merges the two words (rightso) to capture as one word. Can be solved using python regular expressions in future sprint.
- After testing a sample of 20 words, there were no other discrepancies found

3. Negative test : Sample records tested to check if word is appearing in documents other than the ones accumulated in result (word, [doc_ids]) - grep/findstr recursively in linux/windows
4. Duplicate record test : No duplicates in the list of doc_ids and no duplicates in the words and word_ids extracted - Data can be put into microsoft excel and apply conditional formatting to duplicates (This works as amount of data is small).
5. Not null checks : Missing/ Blank words
6. Data test to check if correct mapping of word -> word_id was obtained in Inverted index - Compare data in rdds wordid_dict_rdd and inverted_index_rdd (contains (word, word_id, [doc_ids]) after join operation) to see if the mapping of word -> word_id is consistent in both rdds.


#### Unit functional tests

1. Punctuation removal - Sample words to check if punctuations and special characters are removed from words. Pattern '--' was also replaced with '' which can be undone in the next sprint.
- Observation : Document 9 contains special symbols between words (eg. right--so). Current cleaning process removes the special symbols and merges the two words (rightso).

1.1. Numbers are maintained and are extracted correctly and indexed.

2. Handling of words separated by multiple spaces - We are extracting words by using python split() function which also handles multiple spaces. 
Sample words like 'CHAPTER' which always have multiple spaces before them tested and are being extracted properly. 


#### Data and Functional Testing

For full-fledged data and functional testing, it is always better to develop an automation testing script built on top of python/Shell script to test large chunks of data.

Testing considerations - 
-Inverted index is correctly generated for words and correct files are identified.
-Words ids correctly mapped in the inverted index solution.
-All words are getting extracted and parsed. No missed words - Extract all distinct words from the dataset (can use a NOSQL database) and possibly perform Data Validation and Compare with Words extracted in our solution.


#### Edge cases.
1. Blank File in input dataset is currently processed and handled correctly. Similarly corrupted file in dataset can be tested.
2. After applying deeper text cleaning and stemming, check for edge case words.



## Future scope

Add a more thorough data cleaning process using state of the art Natural Language Processing tasks like stemming.
