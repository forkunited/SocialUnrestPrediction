# SocialUnrestPrediction #

This repository contains code and resources for the following purposes related 
to the unrest prediction task within the OSI project:

1.	Scraping data from Facebook
2.	Featurizing Facebook data for use in a prediction model
3.	Finding users on Twitter which have a large number of unrest related posts

The prediction model in 2 is a logistic regression model which was initially
built for Twitter data.  This model is described in the docs/Twitter_Report.pdf 
file.

The first section below describes how the code is organized, and the second
section describes how to run things.

## Layout of the code ##

The code is organized into the following packages in the src directory:

*	*unrest.detector* - Classes implementing BBN's date 
plus location plus unrest term criteria for detecting unrest posts.

*	*unrest.facebook* - Code for scraping Facebook data (in FacebookScraper), 
code for detecting unrest posts within the scraped data using BBN's criteria 
(in HDetectUnrestFacebookPosts), and code for featurizing the Facebook data so 
that it can be pushed into David's logistic regression model (distributed 
across several classes and called by runFacebookDataFeaturizer.sh in the top 
level directory).

*	*unrest.feature* - Classes implementing features that are currently used
by the featurization code in unrest.facebook to push the Facebook code into
the logistic regression model

*	*unrest.scratch* - Various runnable classes for constructing 
gazetteers, summarizing the data, and other miscellaneous tasks.

*	*unrest.twitter* - Code which uses classes from unrest.detector to find
Twitter accounts which frequently create unrest posts according to BBN's
criteria.

*	*unrest.util* - Utilities.

More information about the classes in these packages is contained at the top
of some of the class files.

## How to run things ##

Before running anything, you need to configure the project for your local 
setup.  To configure, do the following:

1.	Untar files/resources.tar.gz, and put the directory somewhere appropriate.
This directory contains gazetteers and libraries required by the project.

2.  Copy files/build.xml and files/unrest.properties to the top-level directory
of the project. 

3.  Fill out the copied unrest.properties file with the appropriate settings.
It's okay to leave the "hdfs" properties blank if you aren't using any of the
Hadoop parts of the project.

4. Compile the project by running "ant". Build the jar by running ant build-jar. 

### How to run the Facebook scraper ###

You can run the scraper for N iterations by executing the command:

	ant RunFacebookScraper -Diterations="[N]"

### How to run the Facebook featurization ###

You can featurize the Facebook data by running the runFacebookDataFeaturizer.sh
shell script.  For this script to run, you must have set the environment 
variable HDFS_SOCIAL_UNREST_PREDICTION_PROPERTIES to the HDFS path to a copy 
of the unrest.properties file.  This will run the classes under 
unrest.facebook.hadoop to produce a bunch of feature files required by the logistic regression model. 

### How to run the Hadoop jobs for finding Twitter unrest accounts ###

Run the classes under unrest.twitter.hadoop using the commands:

	hadoop jar Unrest.jar unrest.twitter.hadoop.HBBNDetectUsers [HDFS unrest.properties location] [input file] [output file]

	hadoop jar Unrest.jar unrest.twitter.hadoop.HBBNDetectUsersCount [HDFS unrest.properties location] [input file] [output file]

The first command runs a Hadoop job to produce lists of unrest posts for each 
Twitter account, and the second command runs a Hadoop job to produce counts 
of unrest posts for each Twitter account.

