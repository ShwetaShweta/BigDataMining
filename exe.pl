#!/usr/src/perl -w
use strict;
use JSON;
require ETL;
#In this script we will extract 'business_id,name,category,review_count,stars' from the file
#'yelp_academic_dataset_business.json using sub routines from ExtractTransform.pl

my $readBusinessFile = "./yelp/yelp_academic_dataset_business.json";
my $writeBusinessFile = "./CleanedFiles/yelp_academic_business.json";
######################################################################
my $readUserReviewFile = "./yelp/yelp_academic_dataset_review.json";
my $writeUserReviewFile = "./CleanedFiles/yelp_academic_review.json";
my $stopWords = "./StopWords/StopWords.txt";
######################################################################
my $readUserFile = "./yelp/yelp_academic_dataset_user.json";
my $writeUserFile = "./CleanedFiles/yelp_academic_user.json";

#To extract business objects
#ETL::extractBusinessObjects($readBusinessFile,$writeBusinessFile);

#To extract review objects
#ETL::extractReviewObjects($readUserReviewFile,$writeUserReviewFile,$stopWords);

#To extract user objects
ETL::extractUserObjects($readUserFile,$writeUserFile);