# Capston Proyect

### Problem Description

Assume that you work for a user behavior analytics company that collects user data and creates user profiles. You must build a data pipeline to populate the fact_movie_analytics table, this is an OLAP table. The data from fact_movie_analytics is useful for analysts and tools like dashboard software.

The table fact_movie_analytics takes information from:

* A PostgreSQL table named user_purchase. 

* Daily data by an external vendor in a CSV file named movie_review.csv that populates the classified_movie_review table. This file contains a customer id, review id and the message from the movie review. 

* Daily data by an external vendor in a CSV file named log_reviews.csv. This file contains the id review and the metadata about the session when the movie review was done like log date, device (mobile, computer), OS (windows, linux), region, browser, IP, phone number.

### Setup the environment

Use the Terraform template to accommodate the corresponding blocks in order to create the DW.

* EKS cluster

* S3 buckets

* EMR cluster

* Redshift cluster

Terraform Modules and instructions are located in aws folder. 

### Load the data

Use the storage resources S3 created with Terraform.

One bucket for Raw Layer and the other for Staging Layer.

* Upload movie_review.csv and log_reviews.csv files in Raw Layer.

* Upload python scripts files needed to process the data in Raw Layer.

* Upload packages.sh script in Raw Layer (Needed for Bootstrap actions in EMR)

### Process and data Transformation

- Read the PostgreSQL table and write the data in the Staging Layer (dag_s3_to_postgres).

- Read movie_review.csv from Raw Layer and write the result in the Staging Layer following the logic listed below. (Movie review logic).
- Read log_reviews.csv from Raw Layer and write the result in the Staging Layer following the logic listed below. (Log reviews logic).
- Use the EMR cluster created with Terrafomr to build dim tables and calculate fact_movie_analytics (Fact movie analytics logic).



