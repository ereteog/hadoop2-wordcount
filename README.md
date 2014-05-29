hadoop2-wordcount
=================

#WordCount tutorial for hadoop 2.4.0
The original tutorial for hadoop 1.2.1 is here: http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html.The new API require some modifications.

##Compile:
mvn install assembly:single

##Test on Hadoop 2.4.0 

Installation with a Single Node Cluster with Pseudo-Ditributed mode
http://hadoop.apache.org/docs/r2.4.0/hadoop-project-dist/hadoop-common/SingleCluster.html

###launch hadoop:
sbin/start-all.sh

###Add following files to hdfs:

* conf file: patterns.txt:

\\.

\,

\\!

to



bin/hdfs dfs -mkdir /conf

bin/hdfs dfs -put path/to/patterns.txt /conf

* input files

file01:

Hello World, Bye World! 

file02: 

Hello Hadoop, Goodbye to hadoop.


bin/hdfs dfs -mkdir /input

bin/hdfs dfs -put path/to/file01 /input

bin/hdfs dfs -put path/to/file02 /input

###Execution

* case sensitive test
bin/hadoop jar wordcount.jar tutorial.WordCount -D wordcount.case.sensitive=false /input /output -skip /conf/patterns.txt


bin/hdfs dfs -ls /output

/output/_SUCCESS

/output/part-r-00000


bin/hdfs dfs -cat /output/part-r-00000

bye	1

goodbye	1

hadoop	2

hello	2

world	2

* non case sensitive test
bin/hadoop jar wordcount.jar tutorial.WordCount -D wordcount.case.sensitive=true /input /output -skip /conf/patterns.txt

bin/hdfs dfs -cat /output/part-r-00000

Bye	1

Goodbye	1

Hadoop	1

Hello	2

World	2

hadoop	1


