# Week 4 Analytics Engineering with dbt Homework

## Question 1

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup_datetime)?**

You will need to have completed the "Build the First dbt Models" video and have been able to run the models via the CLI. You should find the views and models for querying in your data warehouse.

    61602985

## Question 2

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualizing the Data" videos, either using Google Data Studio or Metabase.

    Yellow taxi: 89.8%
    Green taxi: 10.2%

[Link to Chart](https://datastudio.google.com/s/qyXI8vAkeKY)

## Question 3

**What is the number of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run the model via CLI without limits (is_test_run: false). Filter records with pickup time for year 2019.

    42,084,899

## Question 4

**What is the number of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**

Create a core model for the stg_fhv_tripdata joining with dim_zones. Similar to what was done with fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. Run the model via the CLI without limits (is_test_run: false) and filter records with pickup time in the year 2019.

    22,676,253

## Question 5

**What is the month with the largest number of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

    January

[See Chart](https://datastudio.google.com/s/t8pAXk5gni8)
