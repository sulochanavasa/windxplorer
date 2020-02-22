
import os
import sys
import itertools

sys.path.append("/home/ubuntu/windxplorer/dash")

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql import types

from spark_utils import create_spark_session, write_to_postgres
from wtk_utils import aws_copy_files

from db import get_all_sites

# 5 cores per executor good practice
# Total Cores = 6*3 = 18
# Total executors = 18/5 = 3. So, 1 executor per node
# Memory per node per executor.

SITES_PER_JOB = 50

def move_parquet(site_list, run_id):
    for site in site_list:
        print(site)
        aws_copy_files("%d.parquet"%site, "RUN%s" %run_id)

# Print the number of sites per state in this slice
def print_state_count(site_list, site):
    state_dict = {}
    for s in site_list:
        state = site[s]['state']
        if state not in state_dict:
            state_dict[state] = 1
        else:
            state_dict[state] = state_dict[state] + 1

    for s in state_dict:
        print("STATE %s => %u" % (s, state_dict[s]))
    print("NUMBER OF STATES => %u" % len(state_dict))

def all_lists_empty(lists):
    for l in lists:
        if len(l):
            return False
    return True

# Pick from each state and build a list
def evenly_spaced(site_list_of_lists):
    l = []
    while not all_lists_empty(site_list_of_lists):
        for site_list in site_list_of_lists:
            if len(site_list):
                l.append(site_list.pop(len(site_list)-1))
    return l

def spark_split_jobs():
    # List of states and sites
    state = {}
    site = {}
    db = get_all_sites()
    print("TOTAL SITES %u" % len(db))
    for site_info in db:
        # Accumulate by state
        if not site_info['state'] in state:
            state[site_info['state']] = [site_info['site_id']]
        else:
            state[site_info['state']].append(site_info['site_id'])

        # Reverse index by site_id
        if not site_info['site_id'] in site:
            site[site_info['site_id']] = site_info

    '''
    # Print states and sites
    min_sites = 9999999
    max_sites = 0
    min_state = ""
    max_state = ""
    for s in state.keys():
        num_sites = len(state[s])
        print("STATE: %s SITES: %u" % (s, len(state[s])))
        if s is None:
            continue
        if num_sites < min_sites:
            min_sites = num_sites
            min_state = s
        if num_sites > max_sites:
            max_sites = num_sites
            max_state = s
    print("TOTAL STATES => %u" % (len(state.keys())))
    print("MAX SITES STATE %s SITES %u" % (max_state, max_sites))
    print("MIN SITES STATE %s SITES %u" % (min_state, min_sites))
    '''

    site_list_of_lists = []
    for s in state:
        site_list_of_lists.append(state[s])
        
    # Distributed the states evenly in the list
    zipped_list = evenly_spaced(site_list_of_lists)

    # Break list into chunks
    run_id = 1
    for i in range(0, len(zipped_list), SITES_PER_JOB):
        split_list = zipped_list[i:i+SITES_PER_JOB]
        #print_state_count(split_list, site)
        # Move parquet files to RUNx folder
        move_parquet(split_list, run_id)
        # Run the spark job on the RUNx folder files
        #spark_run_split_job(run_id)
        run_id = run_id + 1
        

def spark_run_split_job(run_id):
    spark = create_spark_session("windexplorer:process_site_data")

    df = spark.read.parquet("s3a://windtoolkit/pywtk-data/RUN%d/*.parquet" % run_id)

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
    df_avg_daily = spark.sql("select site_id, avg(density) as avgDensity, \
                    avg(power) as avgPower, \
                    avg(pressure) as avgPressure, avg(temperature) as avgTemp, \
                    avg(wind_speed) as avgWindspeed, day, month, year \
                    from site_data \
                    group by day,month,year,site_id \
                    order by year, month, day")
    df_avg_daily.createOrReplaceTempView("site_daily_avg_data")

    write_to_postgres(df_avg_daily, 'SITE_AVG_DAILY_CAPACITY', 'ignore')


    #Total Daily power
    df_sumPower_daily = spark.sql("select site_id,sum(power) as dailyPower, \
                     day, month, year \
                     from site_data \
                     group by day,month,year,site_id \
                     order by year,month,day")
    df_sumPower_daily.createOrReplaceTempView("site_dailyPower_data")
    write_to_postgres(df_sumPower_daily, 'SITE_TOTAL_DAILY_CAPACITY', 'ignore')

    #Total monthly power
    df_sumPower_monthly = spark.sql("select site_id, sum(dailyPower) as monthlyPower, month, year \
                      from site_dailyPower_data \
                      group by month,year,site_id \
                      order by year,month")

    df_sumPower_monthly.createOrReplaceTempView("site_monthlyPower_data")
    #Write to database
    write_to_postgres(df_sumPower_monthly, 'SITE_TOTAL_MONTHLY_CAPACITY', 'ignore')

    #Yearly power
    df_sumPower_yearly = spark.sql("select site_id,sum(monthlyPower) as yearlyPower, year \
                       from site_monthlyPower_data \
                       group by year,site_id \
                       order by year")
    df_sumPower_yearly.createOrReplaceTempView("site_yearlyPower_data")
    #Write to database
    write_to_postgres(df_sumPower_yearly, 'SITE_TOTAL_YEARLY_CAPACITY', 'ignore')

    #Total daily power By wind direction
    #To be done

    #Calculate site score
    df_power = spark.sql("select site_id,avg(yearlyPower) as avgPower \
                         from site_yearlyPower_data \
                         where year between 2009 and 2013 \
                         group by site_id")

    df_power.createOrReplaceTempView("site_score")

    SITE_SCORE_RANGE = 10

    df_pd = df_power.toPandas()

    # Get the row with the max/min avg power
    row_idx_max = df_pd['avgPower'].idxmax()
    row_idx_min = df_pd['avgPower'].idxmin()

    # Get the max/min avg power
    max_power = df_pd.loc[row_idx_max]['avgPower']
    min_power = df_pd.loc[row_idx_min]['avgPower']

    # Get the segment for a score for the range you want specified by SITE_SCORE_RANGE
    SEGMENT = (max_power - min_power)/SITE_SCORE_RANGE

    # Create a list to append as column in the end.
    site_score = []

    # Loop over your pandas rows
    for index, row in df_pd.iterrows():
        site_power = row['avgPower']
        #Calculate site score
        score = int((site_power - min_power)/SEGMENT)
        # Store in list
        site_score.append(score)

    df_pd['site_score'] = site_score

    df_final = spark.createDataFrame(df_pd)

    #Write to database
    write_to_postgres(df_final, 'SITE_SCORE', 'ignore')

    '''
    #Group by Wind Direction
    df1 = df.filter(col('wind_direction') == '0') \
            .withColumn('wind_direction',lit('N'))
    df2 = df.filter(col('wind_direction') == '45') \
            .withColumn('wind_direction',lit('NE'))
    df3 = df.filter(col('wind_direction') == '90') \
            .withColumn('wind_direction',lit('E'))
    df4 = df.filter(col('wind_direction') == '135') \
            .withColumn('wind_direction',lit('SE'))
    df5 = df.filter(col('wind_direction') == '180') \
            .withColumn('wind_direction',lit('S'))
    df6 = df.filter(col('wind_direction') == '225') \
            .withColumn('wind_direction',lit('SW'))
    df7 = df.filter(col('wind_direction') == '270') \
            .withColumn('wind_direction',lit('W'))
    df8 = df.filter(col('wind_direction') == '315') \
            .withColumn('wind_direction',lit('NW'))
    df9 = df.filter(((col("wind_direction") > 0) & (col("wind_direction") < 45))) \
            .withColumn('wind_direction',lit('NNE'))
    df10 = df.filter(((col("wind_direction") > 45) & (col("wind_direction") < 90))) \
             .withColumn('wind_direction',lit('ENE'))
    df11 = df.filter(((col("wind_direction") > 90) & (col("wind_direction") < 135))) \
             .withColumn('wind_direction',lit('ESE'))
    df12 = df.filter(((col("wind_direction") > 135) & (col("wind_direction") < 180))) \
             .withColumn('wind_direction',lit('SSE'))
    df13 = df.filter(((col("wind_direction") > 180) & (col("wind_direction") < 225))) \
             .withColumn('wind_direction',lit('SSW'))
    df14 = df.filter(((col("wind_direction") > 225) & (col("wind_direction") < 270))) \
             .withColumn('wind_direction',lit('WSW'))
    df15 = df.filter(((col("wind_direction") > 270) & (col("wind_direction") < 315))) \
             .withColumn('wind_direction',lit('WNW'))
    df16 = df.filter(((col("wind_direction") > 315) & (col("wind_direction") < 360))) \
             .withColumn('wind_direction',lit('NNW'))

    def union_all(dfs):
        if len(dfs) > 1:
            return dfs[0].unionAll(union_all(dfs[1:]))
        else:
            return dfs[0]

    df_all = union_all([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16])

    #Average monthly
    df_wd = df_all.groupby('wind_direction','month','year','site_id').avg('wind_speed','power','temperature', 'pressure','density')
    #Write to database
    write_to_postgres(df_wd, 'SITE_AVG_WIND_DIR', 'ignore')


    #TOtal Monthly/Yearly Power by site_id
    df_wd = df_all.groupby('wind_direction','month','year','site_id').sum('power')
    #Write to database
    write_to_postgres(df_wd, 'SITE_TOT_POWER_WIND_DIR', 'ignore')
    '''

    spark.stop()

if __name__ == "__main__":
    spark_split_jobs()
