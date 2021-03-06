
# Answers to Homework Questions for Week #3

## Question 1

**What is the count for FHV vehicles for the year 2019?**
Can load the data for cloud storage and run a COUNT(*).

    42084899

## Question 2

**How many distinct dispatching_base_num we have in FHV for 2019?**
Can run a distinct query on the table from question 1.

    792

## Question 3

**Best strategy to optimize if query always filter by dropoff_datetime and order by dispatching_base_num.**
Review partitioning and clustering video.
We need to think what will be the most optimal strategy to improve query performance and reduce cost.

## Question 4

**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279?**
Create a table with optimized clustering and partitioning, and run a COUNT(*).  Estimated data processed can be found in the top right corner and actual data processed can be found after the query is executed.

    Count:  26558
    Estimated data processing time:  400.1 MiB processed
    Actual data processing time:  0.5 sec elapsed, 124.4 MB processed

## Question 5

**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag?**
Review partitioning and clustering video.  Partitioning cannot be created on all data types.

    Since creating partitioning with string data.  Only a field that is either a DATE, TIMESTAMP, or DATETIME fields are supported in BigQuery.  Therefore, the best strategy involved creating a cluster with dispatching_base_num and then SR_Flag.

## Question 6

**What improvements can be seen by partitioning and clustering for data size less than 1 GB?**
Partitioning and clustering also creates extra metadata.
Before query exeuction this metadata needs to be processed.

    There will be no improvements or a decrease in performance due to a large metadata.

## Question 7 (Not Required)

**In which format does BigQuery save data?**
Review BigQuery internals video.

    BigQuery stores data in a columar format.  This allows an increase in efficiency with aggregating data.