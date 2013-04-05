
hadoop fs -mkdir ~/hadoop-hdfs
hadoop fs -put test-data/file*.txt ~/hadoop-hdfs

mvn package

java -jar target/hadoop-fun-1.0-SNAPSHOT-jar-with-dependencies.jar  ~/hadoop-hdfs/file1.txt ~/hadoop-hdfs/file2.txt output

