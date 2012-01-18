#!/bin/bash

. $HADOOP_HOME/conf/hadoop-env.sh

hadoop jar a.jar mapreduce.CampaignMR1 $*


