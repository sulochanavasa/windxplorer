import os
import pandas
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql import types
from spark_utils import *
from wtk_utils import *

def process_site_data(site_id):
    # Read from parquet
    df = spark.read.parquet(get_parquet_uri(site_id))

    df = df.withColumn('datetime', df.datetime.cast('date'))
    df = df.withColumn("day", dayofmonth(df["datetime"]))
    df = df.withColumn("month", month(df["datetime"]))
    df = df.withColumn("year", year(df["datetime"]))

    df = df.withColumn('density', round(df["density"],2))
    df = df.withColumn('power', round(df["power"],2))
    df = df.withColumn('pressure', round(df["pressure"],2))
    df = df.withColumn('temperature', round(df["temperature"],2))
    df = df.withColumn('wind_direction', round(df["wind_direction"],2))
    df = df.withColumn('wind_speed', round(df["wind_speed"],2))

    df.createOrReplaceTempView("site_data")

    #Calculate daily avg
    #spark.sql("$select avg(power),day, month,year from site_data group by day,month,year order by year,month,day").show()

    df_avg_daily = spark.sql("select site_id, avg(density) as avgDensity, \
                    avg(power) as avgPower, \
                    avg(pressure) as avgPressure, avg(temperature) as avgTemp, \
                    avg(wind_speed) as avgWindspeed, day, month, year \
                    from site_data \
                    group by day,month,year,site_id \
                    order by year, month, day")
    df_avg_daily.createOrReplaceTempView("site_daily_avg_data")
    write_to_postgres(df_avg_daily, 'SITE_AVG_DAILY_CAPACITY', 'overwrite')


    #Total Daily power
    df_sumPower_daily = spark.sql("select site_id,sum(power) as dailyPower, \
                     day, month, year \
                     from site_data \
                     group by day,month,year,site_id \
                     order by year,month,day")
    df_sumPower_daily.createOrReplaceTempView("site_dailyPower_data")
    write_to_postgres(df_sumPower_daily, 'SITE_TOTAL_DAILY_CAPACITY', 'overwrite')

    #Total monthly power
    df_sumPower_monthly = spark.sql("select site_id, sum(dailyPower) as monthlyPower, month, year \
                      from site_dailyPower_data \
                      group by month,year,site_id \
                      order by year,month")

    df_sumPower_monthly.createOrReplaceTempView("site_monthlyPower_data")
    write_to_postgres(df_sumPower_monthly, 'SITE_TOTAL_MONTHLY_CAPACITY', 'overwrite')

    #Yearly power
    df_sumPower_yearly = spark.sql("select site_id,sum(monthlyPower) as yearlyPower, year \
                       from site_monthlyPower_data \
                       group by year,site_id \
                       order by year")
    df_sumPower_yearly.createOrReplaceTempView("site_yearlyPower_data")
    write_to_postgres(df_sumPower_yearly, 'SITE_TOTAL_YEARLY_CAPACITY', 'overwrite')

    print("Done processing site %d" % site_id)

if __name__ == '__main__':
    spark = create_spark_session('windexplorer:get_site_data')

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # All the wind sites
    sites = pywtk.site_lookup.sites

    # All site-ids
    site_list = [sites['gid'].values[i]-1 for i in range(0,len(sites))]

    # Parallelize
    dist_site_list = spark.sparkContext.parallelize(site_list, len(site_list))

    # Download foreach site
    dist_site_list.foreach(lambda site: process_site_data(site))

    dist_site_list.collect()
