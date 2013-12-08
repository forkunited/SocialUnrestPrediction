#!/bin/bash

#mkdir /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/
#hadoop dfs -mkdir /user/wmcdowell/osi/Data/Facebook/Output

# Filter to posts
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/12-02-2013Posts
#hadoop jar Unrest.jar unrest.facebook.hadoop.HFilterFacebookDataToPosts -D mapred.reduce.tasks=170 /user/wmcdowell/osi/Data/Facebook/12-02-2013 /user/wmcdowell/osi/Data/Facebook/12-02-2013Posts

# Featurize posts
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/12-02-2013Features
#hadoop jar Unrest.jar unrest.facebook.hadoop.HFeaturizeFacebookPosts -D mapred.reduce.tasks=170 /user/wmcdowell/osi/Data/Facebook/12-02-2013Posts/part-r-* /user/wmcdowell/osi/Data/Facebook/12-02-2013Features

# Copy featurized posts to local file system
#rm -rf /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/*
#hadoop dfs -copyToLocal /user/wmcdowell/osi/Data/Facebook/12-02-2013Features/part-r-* /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/
#cat /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/part-r-* > /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013Features

# Count number of posts per date/location
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/12-02-2013DateLocationPostCounts
#hadoop jar Unrest.jar unrest.facebook.hadoop.HAggregateFeaturesByDateLocation -D mapred.reduce.tasks=20 /user/wmcdowell/osi/Data/Facebook/12-02-2013Features/part-r-* /user/wmcdowell/osi/Data/Facebook/12-02-2013DateLocationPostCounts

# Copy counts of posts per date/location to local file system
#rm -rf /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/*
#hadoop dfs -copyToLocal /user/wmcdowell/osi/Data/Facebook/12-02-2013DateLocationPostCounts/part-r-* /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/
#cat /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/part-r-* > /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013DateLocationPostCounts

# Copy merged counts of posts per date/location back to hdfs
#hadoop dfs -copyFromLocal /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013DateLocationPostCounts /user/wmcdowell/osi/Data/Facebook/Output

# Compute mean, sd, and count for each feature term
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/12-02-2013TermAggregates
#hadoop jar Unrest.jar unrest.facebook.hadoop.HAggregateFeaturesByTerm -D mapred.reduce.tasks=20 /user/wmcdowell/osi/Data/Facebook/12-02-2013Features/part-r-* /user/wmcdowell/osi/Data/Facebook/12-02-2013TermAggregates

# Copy mean, sd, and count for each feature term to local file system
#rm -rf /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/*
#hadoop dfs -copyToLocal /user/wmcdowell/osi/Data/Facebook/12-02-2013TermAggregates/part-r-* /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/
#cat /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/part-r-* > /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TermAggregates

# Split mean, sd, and counts for each feature into aggregate files for each language and vocab files for each language/feature
#cd /home/wmcdowell/osi/Projects/SocialUnrestPrediction
#ant FeatureTermAggregateSplitter -DaggregateTermInputPath="/home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TermAggregates" -DfeatureVocabOutputPathPrefix="/home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TermAggregates.vocab" -DfeatureAggregateOutputPathPrefix="/home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TermAggregates.agg"

# Copy split aggregate files to hdfs
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/Output/12-02-2013TermAggregates.*
#hadoop dfs -copyFromLocal /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TermAggregates.* /user/wmcdowell/osi/Data/Facebook/Output/

# Construct training data for unrest prediction model
#hadoop dfs -rmr /user/wmcdowell/osi/Data/Facebook/12-02-2013TrainingData
#hadoop jar Unrest.jar unrest.facebook.hadoop.HConstructTrainingData -D mapred.reduce.tasks=20 /user/wmcdowell/osi/Data/Facebook/12-02-2013Features/part-r-* /user/wmcdowell/osi/Data/Facebook/12-02-2013TrainingData

# Copy training data to local file system
#rm -rf /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/*
#hadoop dfs -copyToLocal /user/wmcdowell/osi/Data/Facebook/12-02-2013TrainingData/part-r-* /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/
#cat /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/part-r-* > /home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TrainingData

# Split training data into separate files for each language/feature
#cd /home/wmcdowell/osi/Projects/SocialUnrestPrediction
#ant TrainingDataSplitter -DtrainingDataInputPath="/home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013TrainingData" -DoutputPathPrefix="/home/wmcdowell/osi/Data/Facebook/12-02-2013/12-02-2013.train"

#rm -rf /home/wmcdowell/osi/Data/Facebook/12-02-2013/tmp/