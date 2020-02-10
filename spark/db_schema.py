from pyspark.sql.types import *

SITE_META_DATA_SCHEMA = StructType([
    StructField('id', StringType(), True),
    StructField('gid', StringType(), True),
    StructField('site_id', StringType(), True),
    StructField('fraction_of_usable_area', StringType(), True),
    StructField('power_curve', StringType(), True),
    StructField('capacity', StringType(), True),
    StructField('wind_speed', StringType(), True),
    StructField('capacity_factor', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('country', StringType(), True),
    StructField('abbreviation', StringType(), True),
    StructField('countryCode', StringType(), True),
    StructField('nextAbbreviation', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('dst', StringType(), True),
    StructField('dstStart', StringType(), True),
    StructField('countryName', StringType(), True),
    StructField('gmtOffset', StringType(), True),
    StructField('dstEnd', StringType(), True),
    StructField('the_geom', StringType(), True),
    StructField('point', StringType(), True),
    StructField('lat', StringType(), True),
    StructField('lon', StringType(), True),
])

WIND_MONTHLY_AVG = StructType([
    StructField('site_id'), StringType(), True),
    StructField('timeStamp'), StringType(), True),
    StructField('month'), StringType(), True),
    StructField('year'), StringType(), True),
    StructField('avg_density', StringType(), True),
    StructField('avg_power', StringType(), True),
    StructField('avg_pressure', StringType(), True),
    StructField('avg_temperature', StringType(), True),
    StructField('wind_direction', StringType(), True),
    StructField('avg_wind_speed', StringType(), True),
])

WIND_YEARLY_AVG_SCHEMA = StructType([
     StructField('site_id'), StringType(), True),
     StructField('year'), StringType(), True),
     StructField('avg_density', StringType(), True),
     StructField('avg_power', StringType(), True),
     StructField('avg_pressure', StringType(), True),
     StructField('avg_temperature', StringType(), True),
     StructField('wind_direction', StringType(), True),
     StructField('avg_wind_speed', StringType(), True),
 ])

WIND_SITE_SCORE_SCHEMA = StructType([
    StructField('site_id', StringType(), True),
    StructField('site_score', StringType(), True),
])

WIND_FORECAST_SCHEMA = StructType([
    StructField('site_id', StringType(), True),
    StructField('TimeStamp', StringType(), True),
    StructField('day_ahead_power', StringType(), True),
    StructField('hour_ahead_power', StringType(), True),
    StructField('4_hour_ahead_power', StringType(), True),
    StructField('6_hour_ahead_power', StringType(), True),
    StructField('day_ahead_power_p90', StringType(), True),
])

