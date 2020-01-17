# EECS 476 StopWord (Project 0) Hadoop Example

One common pre-processing step when analyzing text data is the removal of stop words.  Stop words are those words that appear very commonly across the documents,therefore loosing their representativeness (eg. the, a, and). A typical way to find the stopwords is to measure the number of documents a term appears in and filter the top-k words or those that appear in more than x% of documents. 
In this task, we define the stop words as **the words that appear in ≥ 50% of the documents.**

### Q1. Stop word count
Write a MapReduce program which outputs the stop words in these tweets, and how many times they appear in the documents. Treat each tweet as a separate document. 
We recommend that you start by modifying the MapReduce program ”WordCount”. 
### Q2.   Modifications
1.  Write a MapReduce program which outputs the stop words that appear an even number of times.
2.  Write a MapReduce program which outputs the two most frequent stop words


First clone the repo on Great Lakes:
```
git clone https://github.com/datamining-class/StopWords476
```

Source code can be found in the directory *src/main/java/com/sample/StopWord*

Next build the project: 
```
cd StopWords476
./gradlew clean jar
```
NOTE: if you are running on mac you may have to run: 
```
chmod +x ./gradlew
```

This will produce a jar file with all dependencies and source code in the jar. 
The jar file can be found in the directory: *build/libs/*

To run the project on Great Lakes use the command:
```
hadoop jar build/libs/StopWords476-1.0-SNAPSHOT.jar --input_path <HDFS_INPUT_FILE> --output_path <HDFS_OUTPUT_LOCATION>
```

To run the project locally: 
```
java -jar build/libs/StopWords476-1.0-SNAPSHOT.jar --input_path input/tweets.txt --output_path output
```


If you would like to change the jar file's name change the version number in the file *build.gradle* and the root name
in the file *settings.gradle*

Make sure that *<HDFS_INPUT_FILE>* is a file on HDFS  (or local filesystem if using the second option). Most input files will be found in the */var/eecs476w20/* directory.
Make sure that *<HDFS_OUTPUT_DIRECTORY>* is a directory on HDFS that does NOT already exist. 
To verify that *<HDFS_OUTPUT_DIRECTORY>* does not exist you can use the HDFS filesystem commands: 
```
# acts like a regular fs command
hadoop fs -ls 

# remove the output directory if it already exists
hadoop fs -rm -r -f <HDFS_OUTPUT_DIRECTORY> 
```

If you are running locally, you are free to use regular file system commands to manage these files.
