#sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AKIA46XJ4KJZG4A7KW5R"])
#sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["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"])
#sc._jsc.hadoopConfiguration().set("fs.s3a.region", os.environ["us-west-2"])

import os
import shutil
import pandas
import pywtk
import pywtk.wtk_api as pwtk
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from spark_utils import create_spark_session, write_to_postgres
from wtk_utils import get_wtk_output_uri

# Set attributes you want
attributes = ['density', 'power', 'pressure', 'temperature', 'wind_direction', 'wind_speed']

# List of years 2007-2015
start = pandas.Timestamp('2007-01-01', tz='utc')
end = pandas.Timestamp('2015-01-01', tz='utc')

def get_pandas(id):
    site_data = pywtk.wtk_api.get_wind_data(id, start, end, attributes=attributes, source='nc')
    return site_data

def get_parquet(pd):
    #Change the timestamp index to a column
    pd = pd.reset_index()

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    pqf = spark.createDataFrame(pd)

    return pqf

def write_parquet_to_s3(pq):
    #Write to the S3 bucket
    target = get_wtk_output_uri()
    pq.coalesce(1).write.mode('append').parquet(target)
    #df.write.format('parquet').save(target)

def download_site(site_id):
    print("DOWNLOADING SITE ===> %s" % site_id)
    site_pd = get_pandas(int(site_id))
    site_pq = get_parquet(site_pd)
    write_parquet_to_s3(site_pq)

if __name__ == '__main__':
    require_minimum_pandas_version()
    require_minimum_pyarrow_version()

    spark = create_spark_session('windexplorer:get_site_info')

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # All the wind sites
    sites = pywtk.site_lookup.sites

    # All site-ids
    site_list = [str(sites['gid'].values[i]-1) for i in range(0,len(sites))]
    print site_list

    # Parallelize
    dist_site_list = spark.sparkContext.parallelize(site_list, len(site_list))

    # Download foreach site
    dist_site_list.foreach(download_site)

