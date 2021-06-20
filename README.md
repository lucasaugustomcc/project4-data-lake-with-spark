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

## EMR Environment configuration
Add to the ~/.bashrc file.

```bash
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
```

## How to run the Python scripts
1. Connect to the EMR cluster: ssh -i key.pem hadoop@ecX-X-X-X-X.compute-1.amazonaws.com
1. Run *etl.py* to execute the ETL with Spark: python etl.py