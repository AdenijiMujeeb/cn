import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark_script.driver import SparkOperations
import pytest
import shutil
import os

@pytest.fixture(scope="module")
def spark_session():
    """Create a SparkSession object for testing"""
    spark = SparkSession.builder.master("local[1]").appName("testApp").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def test_data(spark_session):
    """Load test data into Spark DataFrames"""
    races = [
        Row(raceId=1, year=2018),
        Row(raceId=2, year=2019),
        Row(raceId=3, year=2018),
    ]
    results = [
        Row(raceId=1, driverId=1, milliseconds=55000, number=1),
        Row(raceId=1, driverId=2, milliseconds=60000, number=2),
        Row(raceId=2, driverId=1, milliseconds=55000, number=1),
        Row(raceId=2, driverId=2, milliseconds=65000, number=2),
        Row(raceId=3, driverId=1, milliseconds=48000, number=1),
        Row(raceId=3, driverId=2, milliseconds=58000, number=2),
    ]
    drivers = [
        Row(driverId=1, dob="1990-01-01", driverRef="driver1", forename="John", surname="Doe"),
        Row(driverId=2, dob="1995-01-01", driverRef="driver2", forename="Jane", surname="Doe"),
    ]
    races_df = spark_session.createDataFrame(races)
    results_df = spark_session.createDataFrame(results)
    drivers_df = spark_session.createDataFrame(drivers)
    return races_df, results_df, drivers_df

def test_read_data(spark_session):
    spark_ops = SparkOperations(spark_session)
    races_df, results_df, drivers_df = spark_ops.read_data()
    assert races_df.count() > 0
    assert results_df.count() > 0
    assert drivers_df.count() > 0

def test_compute_drivers_pit_stop_times(test_data, spark_session):
    """Test compute_pit_stop_times method"""
    races_df, results_df, drivers_df = test_data
    spark_ops = SparkOperations(spark_session)
    # remove any existing output files
    if os.path.exists("./output/drivers_pit_stop_times"):
        shutil.rmtree("./output/drivers_pit_stop_times")
    # call the compute_pit_stop_times method
    pit_stop_times_df = spark_ops.compute_drivers_pit_stop_times(results_df)
    # create the output directory
    os.makedirs("./output/drivers_pit_stop_times", exist_ok=True)
    # write the output files
    pit_stop_times_df.write.format("csv").mode("overwrite").option("header", "true").save("../output/drivers_pit_stop_times")
    # read the output files generated by the method
    pit_stop_times_df = spark_session.read.format("csv").option("header", "true").load("../output/drivers_pit_stop_times")
    # perform any assertions on the output
    # run assertions on the output data
    assert float(pit_stop_times_df.where("raceId == 1").select("avg_pit_stop_time").collect()[0]["avg_pit_stop_time"]) == 55000.0
    assert float(pit_stop_times_df.where("raceId == 2").select("avg_pit_stop_time").collect()[0]["avg_pit_stop_time"]) == 65000.0

