import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

from wtk_utils import get_timezone_uri, get_metadata_uri, get_geodata_uri
from spark_utils import create_spark_session, write_to_postgres

def get_timezone_df(spark):
    url = get_timezone_uri()
    print("Getting timezone from %s" % url)
    df1 = spark.read.load(url, format="csv", delimiter=",",
                          inferSchema="true", header="true")
    df1 = df1.withColumn('timestamp', df1.timestamp.cast('timestamp'))
    df1 = df1.withColumn('dstStart', df1.dstStart.cast('timestamp'))
    df1 = df1.withColumn('dstEnd', df1.dstEnd.cast('timestamp'))

    return df1

def get_metadata_df(spark):
    url = get_metadata_uri()
    print("Getting metadata from %s" % url)
    df2 = spark.read.load(url, format="csv", delimiter=",",
                          inferSchema="true", header="true")
    df2 = df2.withColumn('power_curve', df2.power_curve.cast('INT'))

    return df2

def get_geodata_df(spark):
    url = get_geodata_uri()
    print("Getting geodata from %s" % url)
    df3 = spark.read.load(url, format="csv", delimiter=",",
                          inferSchema="true", header="true")

    return df3

def get_site_info_df(spark):
    timezone_df = get_timezone_df(spark)
    metadata_df = get_metadata_df(spark)
    geodata_df = get_geodata_df(spark)

    timezone_df.createOrReplaceTempView("site_timezones")
    metadata_df.createOrReplaceTempView("site_3tier")
    geodata_df.createOrReplaceTempView("site_geo")

    # TODO(): Is this used at all?
    sql = """
            SELECT DISTINCT site_id, countryName
            FROM sites
            WHERE site_id IS NOT NULL
        """

    sql = """
                SELECT stz.site_id,st.gid,st.fraction_of_usable_area, st.power_curve,\
                    st.capacity,st.wind_speed,\
                    st.capacity_factor,st.city,st.state,st.country,stz.abbreviation,\
                    stz.countryCode,\
                    stz.nextAbbreviation,stz.timestamp,stz.dst,stz.dststart,stz.gmtOffset,\
                    stz.dstEnd,st.the_geom,sg.lat,sg.lon
                FROM site_timezones stz
                INNER JOIN site_3tier st ON stz.site_id = st.site_id
                INNER JOIN site_geo sg ON stz.site_id = sg.site_id
        """

    df = spark.sql(sql)
    df.createOrReplaceTempView("site_info")

    return df

if __name__ == '__main__':
    spark = create_spark_session('windexplorer:get_meta_data')

    # Get the site info df from all the CSV's
    df = get_site_info_df(spark)

    df.show()

    # Write the site info df to db
    write_to_postgres(df, 'site_info', 'overwrite')
