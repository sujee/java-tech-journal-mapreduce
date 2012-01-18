#!/bin/bash

if [ -z "$HADOOP_HOME" ] 
then
    echo "HADOOP_HOME is not set...exiting"
    exit 1
fi

CLASS_DIR=classes2
mkdir -p $CLASS_DIR
rm -rf $CLASS_DIR/*


javac -d $CLASS_DIR  -sourcepath src -classpath $HADOOP_HOME/conf:$HADOOP_HOME/lib/*:$HADOOP_HOME/*:lib/*  $(find src -name "*.java")


rm  -f a.jar
jar cf a.jar -C $CLASS_DIR .

