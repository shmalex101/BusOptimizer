# BusOptimizer

## Motivation
Every day over 400,000 people in Seattle ride the bus. The majority of those people are on the bus for over two hours a day. Everyone wants to improve bus service, but where do you start? Most organizations keep bus gps data, but this can add up to over 400 GB per year. For my insight data engineering project I built a data pipeline to help transit companies utilize this data and help them focus on the bus routes most in need of improvement. This tool is not only useful for public transit, but can be used with any transit fleet equipped with real-time GPS data like delivery services, shipping companies and ride-share services.

# Features

# Tech Stack

![BusOptimizer tech stack](https://github.com/shmalex101/BusOptimizer/blob/master/tech_pipeline.png)

A few notes;
* Postgres installed using [simply install: PostgreSQL](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252) from the insight blog
* The spark cluster was setup using [Pegasus](https://github.com/InsightDataScience/pegasus) with assistance from [How to get Hadoop and Spark up and running on AWS](
https://blog.insightdatascience.com/how-to-get-hadoop-and-spark-up-and-running-on-aws-7a1b0ab55459) on the Insight blog.

## Data Source

The [historical bus data](http://web.mta.info/developers/MTA-Bus-Time-historical-data.html) was made available by the New York City MTA. The data is archived as compressed .csv files and was downloaded using Python and Pyspark. The compressed files were unpacked, converted to Parquet files and moved into an S3 bucket. 

The dataset contains archived MTA Bus Time Data from August 1, 2014 through October 31, 2014 with data for each day saved to a single file for a total of 90 files. The entire dataset adds up to approximately 70 GB.
