"""
If you plan on using PySpark, please use the requirements.txt file at the root directory
of the project for a list of dependencies.

You are free to the code copy from "import findspark" to "spark.read()", we
wanted to ensure that you are able to focus on the business logic and not
installing Spark related items.
"""
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, col, min, max, upper

class SparkOperations:
    
    def __init__(self):
        findspark.init()
        #self.spark = spark        
        self.spark = SparkSession.builder.master("local[1]").appName("testApp").getOrCreate()
        
    def read_data(self):
        try:
            races_df = self.spark.read.format("csv").option("header", "true").load("../data/races.csv")
            results_df = self.spark.read.format("csv").option("header", "true").load("../data/results.csv")
            drivers_df = self.spark.read.format("csv").option("header", "true").load("../data/drivers.csv")
        except Exception as e:
            print(f"Error occurred while reading data: {e}")
            raise
        return races_df, results_df, drivers_df
    
    def compute_drivers_pit_stop_times(self, results_df):
        try:
            drivers_pit_stop_times_df = results_df.select("raceId", "driverId", "milliseconds") \
                .groupBy("raceId", "driverId") \
                .agg(avg("milliseconds").alias("avg_pit_stop_time"))
        
            drivers_pit_stop_times_df.write.format("csv") \
                .partitionBy("raceId") \
                .save("../output/drivers_pit_stop_times", header=True)
        
            return drivers_pit_stop_times_df
        except Exception as e:
            print(f"Error occurred while computing pit stop times: {e}")
            return None
        
    def insert_missing_driver_codes(self, results_df, drivers_df):
        try:
            results_with_codes_df = results_df.withColumnRenamed("number", "result_number") \
                .join(drivers_df, "driverId", "left") \
                .withColumn("code", when(col("code").isNull(), upper(col("driverRef").substr(1, 3))).otherwise(col("code")))
            
            results_with_codes_df.write.format("csv") \
                .option("header", True) \
                .partitionBy("raceId") \
                .save("../output/results_with_codes", header=True)
        except Exception as e:
            print(f"Error occurred while inserting missing driver codes: {e}")
            raise
    
    def compute_drivers_age(self, races_df, results_df, drivers_df):
        try:
            selected_season = "2018"
            races_df_filtered = races_df.filter(col("year") == selected_season)
            
            season_results_df = results_df.join(races_df_filtered, "raceId", "left") \
                .repartition("raceId")
            
            start_season_df = season_results_df.groupBy("driverId").agg(min("date").alias("start_date")) 
            end_season_df = season_results_df.groupBy("driverId").agg(max("date").alias("end_date"))
            
            age_df = drivers_df.join(start_season_df, "driverId", "left") \
                .join(end_season_df, "driverId", "left") \
                .withColumn("start_age", col("start_date").substr(1, 4) - col("dob").substr(1, 4)) \
                .withColumn("end_age", col("end_date").substr(1, 4) - col("dob").substr(1, 4)) \
                .select("driverId", "forename", "surname", "start_age", "end_age")
                
            age_df.write.partitionBy("driverId") \
                .format("csv").save("../output/drivers_age", header=True)
        except Exception as e:
            print(f"Error occurred while computing driver age: {e}")
            raise

    def run_transformation_methods(self):
        races_df, results_df, drivers_df = self.read_data()
        self.compute_drivers_pit_stop_times(results_df)
        self.insert_missing_driver_codes(results_df, drivers_df)
        self.compute_drivers_age(races_df, results_df, drivers_df)

if __name__ == "__main__":
    spark_ops = SparkOperations()
    spark_ops.run_transformation_methods()