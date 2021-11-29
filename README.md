# Data lakes with Spark and AWS S3

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, an ETL pipeline is built to extract data from S3, transform it using Spark and Dataframes and load it back to an ERM cluster running on AWS S3. This process will enable the analytics team as Sparkify to understand what songs the users are listening.

### Setup

- Install python `3.x.x`
- Install pyspark with pip
- Run an ERM cluster in AWS S3
- Add your AWS key, AWS secret key and Cluster url to the `dl.cfg` file.
