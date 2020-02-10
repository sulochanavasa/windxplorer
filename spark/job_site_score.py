import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

from wtk_utils import get_timezone_uri, get_metadata_uri, get_geodata_uri
from spark_utils import create_spark_session, write_to_postgres

def calculate_site_score():
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
    write_to_postgres(df_final, 'SITE_SCORE', 'overwrite')

if __name__ == '__main__':
    spark = create_spark_session('windexplorer:get_site_score')

    calculate_site_score()
