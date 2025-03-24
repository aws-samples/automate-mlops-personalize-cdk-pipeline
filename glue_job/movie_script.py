#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  software and associated documentation files (the "Software"), to deal in the Software
#  without restriction, including without limitation the rights to use, copy, modify,
#  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
This module is a AWS Glue job that processes the MovieLens dataset and generates
three CSV files: interactions.csv, item-meta.csv, and users.csv. These files are
then uploaded to an Amazon S3 bucket for use with the Amazon Personalize service.

The interactions.csv file contains user-item interaction data, with columns for
user ID, item ID, timestamp, and event type (Watch or Click).

The item-meta.csv file contains metadata for each movie item, including the item ID,
genres, year, and a creation timestamp.

The users.csv file contains a list of unique user IDs and randomly assigned genders.

The module downloads the MovieLens dataset, extracts the relevant data, and performs
the necessary transformations to create the required CSV files. It also handles the
upload of these files to the specified S3 bucket.
"""

import sys
import tempfile

from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import numpy as np
from pyspark import SparkFiles
import zipfile
import os
import pandas as pd
import boto3

# Create a SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job_arguments = sys.argv[1:]

# Bucket name for storing data, change bucket name below
bucket_name = "<BUCKET_NAME>"

# Download the MovieLens dataset
zip_url = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
spark.sparkContext.addFile(zip_url)
downloads_zip_path = SparkFiles.get(zip_url.split("/")[-1])

# Extract the downloaded zip file
with tempfile.TemporaryDirectory(prefix="extracted_files_") as temp_dir:
    # Set proper permissions (readable and writable only by the owner)
    os.chmod(temp_dir, 0o644)

with zipfile.ZipFile(downloads_zip_path, 'r') as zip_ref:
    zip_ref.extractall(temp_dir)

extracted_files = []

for root, dirs, files in os.walk(temp_dir):
    for file in files:
        extracted_files.append(os.path.join(root, file))

root_dir = root

# Load the ratings data
original_data = pd.read_csv(root_dir + '/ratings.csv')
original_data.head(5)

# Get an arbitrary timestamp for later use
arb_time_stamp = original_data.iloc[50]['timestamp']

# Create a DataFrame for watched movies (rating > 3)
watched_df = original_data.copy()
watched_df = watched_df[watched_df['rating'] > 3]
watched_df = watched_df[['userId', 'movieId', 'timestamp']]
watched_df['EVENT_TYPE'] = 'Watch'
watched_df.head()

# Create a DataFrame for clicked movies (rating > 1)
clicked_df = original_data.copy()
clicked_df = clicked_df[clicked_df['rating'] > 1]
clicked_df = clicked_df[['userId', 'movieId', 'timestamp']]
clicked_df['EVENT_TYPE'] = 'Click'
clicked_df.head()

# Combine watched and clicked DataFrames
interactions_df = clicked_df.copy()
interactions_df = interactions_df.append(watched_df)
interactions_df.sort_values("timestamp", axis=0, ascending=True,
                            inplace=True, na_position='last')
interactions_df.rename(columns={'userId': 'USER_ID', 'movieId': 'ITEM_ID',
                                'timestamp': 'TIMESTAMP'}, inplace=True)

# Save the interactions data to a CSV file and upload to S3
interactions_filename = "interactions.csv"
interactions_df.to_csv((root_dir + "/" + interactions_filename), index=False, float_format='%.0f')

boto3.Session().resource('s3').Bucket(bucket_name).Object(interactions_filename).upload_file(
    root_dir + "/" + interactions_filename)

# Load the movies data
original_data = pd.read_csv(root_dir + '/movies.csv')
original_data['year'] = original_data['title'].str.extract('.*\((.*)\).*', expand=False)
original_data.head(5)
original_data = original_data.dropna(axis=0)
original_data.isnull().sum()

# Create a DataFrame for item metadata
itemmetadata_df = original_data.copy()
itemmetadata_df = itemmetadata_df[['movieId', 'genres', 'year']]
itemmetadata_df.head()

# Add a creation timestamp for the item metadata
ts = datetime(2022, 1, 1, 0, 0).strftime('%s')
itemmetadata_df['CREATION_TIMESTAMP'] = ts
itemmetadata_df.rename(columns={'genres': 'GENRES', 'movieId': 'ITEM_ID', 'year': 'YEAR'}, inplace=True)

# Save the item metadata to a CSV file and upload to S3
items_filename = "item-meta.csv"
itemmetadata_df.to_csv((root_dir + "/" + items_filename), index=False, float_format='%.0f')
boto3.Session().resource('s3').Bucket(bucket_name).Object(items_filename).upload_file(root_dir + "/" + items_filename)

# Get all unique user IDs from the interactions dataset
user_ids = interactions_df['USER_ID'].unique()
user_data = pd.DataFrame()
user_data["USER_ID"] = user_ids

# Assign random genders to users
possible_genders = ['female', 'male']
random = np.random.choice(possible_genders, len(user_data.index), p=[0.5, 0.5])
user_data["GENDER"] = random

# Save the user data to a CSV file and upload to S3
users_filename = "users.csv"
user_data.to_csv((root_dir + "/" + users_filename), index=False, float_format='%.0f')
boto3.Session().resource('s3').Bucket(bucket_name).Object(users_filename).upload_file(root_dir + "/" + users_filename)

# Commit the Glue job
job.commit()
