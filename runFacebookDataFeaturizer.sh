#!/bin/bash

echo "Using properties file: $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES"

. unrest.properties

facebookTempDirPath="$facebookLocalOutputDirPath/Temp"
facebookHdfsTempDirPath="$facebookHdfsOutputDirPath/Temp"
facebookFeatureOutputPath="$facebookLocalOutputDirPath/$facebookFileNamePrefix""Features"
facebookFeatureSRegOutputPath="$facebookLocalOutputDirPath/$facebookFileNamePrefix""FeatureSReg"
facebookDateLocationPostCountsOutputPath="$facebookLocalOutputDirPath/$facebookFileNamePrefix""DateLocationPostCounts"
facebookTermAggregatesOutputPath="$facebookLocalOutputDirPath/$facebookFileNamePrefix""TermAggregates"
facebookTrainingDataOutputPath="$facebookLocalOutputDirPath/$facebookFileNamePrefix""TrainingData"
facebookTrainingDataOutputSplitsPathPrefix="$facebookLocalOutputDirPath/$facebookFileNamePrefix"".train"

mkdir $facebookTempDirPath
hadoop dfs -mkdir $facebookHdfsTempDirPath

# Filter to posts
hadoop dfs -rmr $facebookHdfsTempDirPath/Posts
hadoop jar Unrest.jar unrest.facebook.hadoop.HFilterFacebookDataToPosts -D mapred.reduce.tasks=170 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsInputDirPath $facebookHdfsTempDirPath/Posts

# Featurize posts
#hadoop dfs -rmr $facebookHdfsTempDirPath/Features
#hadoop jar Unrest.jar unrest.facebook.hadoop.HFeaturizeFacebookPosts -D mapred.reduce.tasks=170 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsTempDirPath/Posts/part-r-* $facebookHdfsTempDirPath/Features

# Copy featurized posts to local file system
#rm -rf $facebookTempDirPath/*
#hadoop dfs -copyToLocal $facebookHdfsTempDirPath/Features/part-r-* $facebookTempDirPath
#cat $facebookTempDirPath/part-r-* > $facebookFeatureOutputPath

# Featurize posts for sentence regularizer
#hadoop dfs -rmr $facebookHdfsTempDirPath/FeatureSReg
#hadoop jar Unrest.jar unrest.facebook.hadoop.HFeaturizeFacebookPostSReg -D mapred.reduce.tasks=170 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsTempDirPath/Posts/part-r-* $facebookHdfsTempDirPath/FeatureSReg

# Copy sentence-regularization featurized posts to local file system
#rm -rf $facebookTempDirPath/*
#hadoop dfs -copyToLocal $facebookHdfsTempDirPath/FeatureSReg/part-r-* $facebookTempDirPath
#cat $facebookTempDirPath/part-r-* > $facebookFeatureSRegOutputPath

# Aggregate sentence-regularization featurized posts into JSON objects for each date/location
#cd $socialUnrestPredictionProjectDirPath
#ant AggregateSRegFeatures -DinputPath="$facebookFeatureSRegOutputPath" -DoutputPath="$facebookFeatureSRegOutputPath.agg"

# Count number of posts per date/location
#hadoop dfs -rmr $facebookHdfsTempDirPath/DateLocationPostCounts
#hadoop jar Unrest.jar unrest.facebook.hadoop.HAggregateFeaturesByDateLocation -D mapred.reduce.tasks=20 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsTempDirPath/Features/part-r-* $facebookHdfsTempDirPath/DateLocationPostCounts

# Copy counts of posts per date/location to local file system
#rm -rf $facebookTempDirPath/*
#hadoop dfs -copyToLocal $facebookHdfsTempDirPath/DateLocationPostCounts/part-r-* $facebookTempDirPath
#cat $facebookTempDirPath/part-r-* > $facebookDateLocationPostCountsOutputPath

# Copy merged counts of posts per date/location back to hdfs
#hadoop dfs -copyFromLocal $facebookPostDateLocationCountsOutputPath $facebookHdfsOutputDirPath

# Compute mean, sd, and count for each feature term
#hadoop dfs -rmr $facebookHdfsTempDirPath/TermAggregates
#hadoop jar Unrest.jar unrest.facebook.hadoop.HAggregateFeaturesByTerm -D mapred.reduce.tasks=20 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsTempDirPath/Features/part-r-* $facebookHdfsTempDirPath/TermAggregates

# Copy mean, sd, and count for each feature term to local file system
#rm -rf $facebookTempDirPath/*
#hadoop dfs -copyToLocal $facebookHdfsTempDirPath/TermAggregates/part-r-* $facebookTempDirPath
#cat $facebookTempDirPath/part-r-* > $facebookTermAggregatesOutputPath

# Split mean, sd, and counts for each feature into aggregate files for each language and vocab files for each language/feature
#cd $socialUnrestPredictionProjectDirPath
#ant FeatureTermAggregateSplitter -DaggregateTermInputPath="$facebookTermAggregatesOutputPath" -DfeatureVocabOutputPathPrefix="$facebookTermAggregatesOutputPath.vocab" -DfeatureAggregateOutputPathPrefix="$facebookTermAggregatesOutputPath.agg"

# Copy split aggregate files to hdfs
#hadoop dfs -rmr $facebookHdfsOutputDirPath/TermAggregates.*
#hadoop dfs -copyFromLocal $facebookTermAggregatesOutputPath.* $facebookHdfsOutputDirPath

# Construct training data for unrest prediction model
#hadoop dfs -rmr $facebookHdfsTempDirPath/TrainingData  
#hadoop jar Unrest.jar unrest.facebook.hadoop.HConstructTrainingData -D mapred.reduce.tasks=20 $HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES $facebookHdfsTempDirPath/Features/part-r-* $facebookHdfsTempDirPath/TrainingData

# Copy training data to local file system
#rm -rf $facebookTempDirPath/*
#hadoop dfs -copyToLocal $facebookHdfsTempDirPath/TrainingData/part-r-* $facebookTempDirPath
#cat $facebookTempDirPath/part-r-* > $facebookTrainingDataOutputPath

# Split training data into separate files for each language/feature
#cd $socialUnrestPredictionProjectDirPath
#ant TrainingDataSplitter -DtrainingDataInputPath="$facebookTrainingDataOutputPath" -DoutputPathPrefix="$facebookTrainingDataOutputSplitsPathPrefix"

#rm -rf $facebookTempDirPath