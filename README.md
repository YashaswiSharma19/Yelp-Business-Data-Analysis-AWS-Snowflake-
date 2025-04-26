# Yelp-Business-Data-Analysis-AWS-Snowflake-
Built a scalable data pipeline using AWS S3 and Snowflake to ingest, store, and analyze over 7 million Yelp reviews for business insights.
step 1 - The original Yelp dataset (7M+ rows) was preprocessed and split into multiple CSV chunks using a custom Python script to optimize upload and ingestion performance.
step 2 - Using Python’s boto3 and concurrent.futures, the dataset chunks were uploaded in parallel to an AWS S3 bucket, reducing the upload time by approximately 60% compared to sequential uploading.
step 3 - An external stage was configured in Snowflake to connect with the S3 bucket, allowing direct access to the uploaded data without manual downloads.
step 4 - Snowflake’s COPY INTO command was used to ingest the data from S3 into a Snowflake table, followed by schema validation, deduplication, and data quality checks.
step 5 - SQL queries were executed in Snowflake to extract insights related to business categories, ratings, and user reviews. 

