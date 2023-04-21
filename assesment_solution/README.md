# Solution
The solution implemented is divided into two parts:
- Solution (can be found in `app_source` folder).
- Test code (can be found in `tests` folder).
I used TDD (Test driven development) approach, that was why I wrote some unit tests.
The assessment solution logics can be found in `app_source/pyspark_script/driver.py`
The output data subsets can be found in `app_source/output`
The test written for this implementation can be found in `tests/code_test.py`

## The unit test
The Python script for testing the Spark application code is located in "app_source/tests". 
It consists of two test functions: "test_read_data", which tests if the data was successfully read from the data source file, and "test_compute_drivers_pit_stop_times", which tests the logic against the expected output. These tests ensure that the application
functions as expected and helps catch any errors or bugs in the code.

## My thought process
The assessment consist of three questions for option 1. The following highlights the questions and the implementation made:
- **What was the average time each driver spent at the pit stop for any given race?**:
    ```
    The solution to this is present in this method (compute_drivers_pit_stop_times)
    This was calculated by selecting the columns we will be needing from the dataframe i.e., "raceId", "driverId", "milliseconds"
    And then grouping the dataframe using the .groupby clause in other to have distinct records for each driver and then, an aggregate of the average time used at the pit stop is calculated using .agg and .avg clause. Then, the generated dataset was partitioned by the "raceId" (since the question referred to "any given race") for performance optimization by splitting up large datasets into smaller chunks. Then, the generated dataset was saved into "app_source/output/drivers_pit_stop_times"
    
    ```
- **Insert the missing code (e.g: ALO for Alonso) for all drivers**:
    ```
    The solution to this is present in this method (insert_missing_driver_codes). The column "number" was renamed to be "result_number" in results dataframe because the same column name exist in drivers dataframe. Then, a left join was used to join drivers_df to results_df because I want to include all the results in "results_with_codes_df" even if the drivers information is missing. Then, .substr clause was used to remove the first three letters of "driverRef" and coverted them to uppercase where there missing code values for drivers. Then, the generated dataset was partitioned by the "raceId" for performance optimization by splitting up large datasets into smaller chunks.
    Then, the generated dataset was saved into "app_source/output/results_with_codes"
    
    ```
- **Select a season from the data and determine who was the youngest and oldest at the start of the season and the end of the season**:
    ```
    The solution to this is present in this method (compute_drivers_age). A season was selected i.e 2018 and this was used to filter the races dataframe. Then, the filtered races df was joined to the result df using a left join because I want to include all the results in "season_results_df" even if the filtered race doesnt exist. Then, two dataframes were created i.e., start_season_df and end_season_df which contains the start and end dates of the season for each driver. The two newly craeted dataframes were joined to the drivers dataframe using a left join because I want to return all the drivers into "age_df" even if start_season_df end_season_df doesnt exist. Then, the year values from column "dob" was substracted from "start_date" in other to find the 'start_age' and the year values from column "dob" was substracted from "end_date" in other to find the 'end_age'. Then, the generated dataset was partitioned by the "raceId" for performance optimization by splitting up large datasets into smaller chunks.mThen, the generated dataset was saved into "app_source/output/drivers_age".
    
    ```



## How to test
To generate the output dataset run the following commands

```
cd ./app_source/pyspark_script
run python driver.py 

```


# Bonus question (Prediction)
The solution to the prediction exercise can be found in "prediction.txt"