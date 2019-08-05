#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`
export CURRENT=Task4

echo --- Deleting
rm $CURRENT.jar
rm $CURRENT*.class

echo --- Compiling
$JAVA_HOME/bin/javac $CURRENT.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $CURRENT.jar $CURRENT*.class

echo --- Running
INPUT=/tmp/smalldata.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time yarn jar $CURRENT.jar $CURRENT -D mapreduce.map.java.opts=-Xmx4g $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT