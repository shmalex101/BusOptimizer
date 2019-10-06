#!/bin/bash

#set environmental variables for database
export DATABASE_NAME="postgres"
export HOSTNAME="<ec2 DNS> "
export USER="alex"
export POSTGRES_PASSWORD="user password"

# command to run batch_process.py
# *** replace <DNS> with DNS for ec2 spark master instance 
spark-submit --master spark:<DNS>:7077 --packages org.postgresql:postgresql:42.1.1 --executor-memory 1g  --driver-memory 1gbatch_process.py

# command to run batch_process_parquet.py
# *** replace <DNS> with DNS for ec2 spark master instance 
spark-submit --master spark:<DNS>:7077 --packages org.postgresql:postgresql:42.1.1 --executor-memory 1g  --driver-memory 1g batch_process_parquet.py
