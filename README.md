# Large Scale Text Data Similarity
This project solve the problem of large-scale text data similarity computing using **Apache Spark** and the **LSH algorithm**.

This repository is created by Stephane Miguel KAKANAKOU. For any questions or suggestion, you can contact me at Skakanakou@gmail.com

## What is the goal of this project? 
The goal of this project is, give a large set of documents, find the similars pairs of documents in a very efficient time. 
<br>For that, I use **Apache Spark** with **LSH**. And as data, I use **Wikipedia articles**. The data is available on the following link [wikispeedia_articles_plaintext.tar.gz](http://snap.stanford.edu/data/wikispeedia.html)

## Implementation Details
I can divide the implementation of the project into two mains parts. In the first part, I prepare our data and in the second part, I perform the LSH to find candidates pairs and then use the Jaccard similarity to find the similars documents.

### Preparation Part
In the preparation part, I have the following steps : 

* **STEP 1** : First I have written code to find and save all the primes numbers smaller than 100000. Here is the link to the java file [FindPrimesNumbers](https://github.com/MiguelSteph/LargeScaleTextSimilarity/blob/master/LargeScaleTextSimilarity/src/main/java/com/findsimilar/preparation/FindPrimeNumbers.java).
* **STEP 2** : I have created a class that contains all the hashing method that I need in the project. In this project, I use the Universal hashing technique to hash integer and I use the DJB2 hashing technique to hash String and also list of integer. Here is the link to the java code [Hasher](https://github.com/MiguelSteph/LargeScaleTextSimilarity/blob/master/LargeScaleTextSimilarity/src/main/java/com/findsimilar/hash/Hasher.java).
* **STEP 3** : I have converted each document of the dataset in a set of k-shingle and hash each k-shingle in token. So during this step, I have transformed each document of the dataset into set of tokens. The set of tokens fit well in memory. Here is the link to the java file [DataPreparation](https://github.com/MiguelSteph/LargeScaleTextSimilarity/blob/master/LargeScaleTextSimilarity/src/main/java/com/findsimilar/preparation/DataPreparation.java).

### Perform LSH
In this part, I have the following steps : 

* **STEP 1**: Load the tokens that I get from the previous part
* **STEP 2**: Compute the signature matrix
* **STEP 3**: Divide the signature matrix into bands and compute the hash value of each document in each band
* **STEP 4**: Identify the candidate pairs
* **STEP 5**: Find the similars documents by computing the jaccard similarity between candidate pair documents.
* **STEP 6**: Save the result into a file


 

