import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

from wtk_utils import get_timezone_uri, get_metadata_uri, get_geodata_uri
from spark_utils import create_spark_session, write_to_postgres

def calculate_wind_direction():
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
    write_to_postgres(df_wd, 'SITE_AVG_WIND_DIR', 'overwrite')


    #Total Monthly/Yearly Power by site_id
    df_wd = df_all.groupby('wind_direction','month','year','site_id').sum('power')
    #Write to database
    write_to_postgres(df_wd, 'SITE_TOT_POWER_WIND_DIR', 'overwrite')

if __name__ == '__main__':
    spark = create_spark_session('windexplorer:get_wind_direction')

    calculate_wind_direction()
