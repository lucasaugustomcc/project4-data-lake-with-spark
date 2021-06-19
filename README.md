# Project: Data Lake with Spark and S3
## Summary of the Project
Sparkify is a startup which wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 

## Files in the Repository
The project includes three files:
- *etl.py*: loads data from S3 into tables using Spark and then save that data into S3.

## Configuration File
```INI
[default]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

## How to run the Python scripts
1. Run *etl.py* to execute the ETL with spark.