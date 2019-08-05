#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"
export CURRENT=Task4

echo --- Deleting
rm $CURRENT.jar
rm $CURRENT*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g $CURRENT.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $CURRENT.jar $CURRENT*.class

echo --- Running
INPUT=/tmp/smalldata.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata.txt /user/zcliang/
time spark-submit --master yarn --class $CURRENT --driver-memory 4g --executor-memory 4g $CURRENT.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
