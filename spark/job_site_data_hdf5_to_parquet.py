#sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AKIA46XJ4KJZG4A7KW5R"])
#sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["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"])
#sc._jsc.hadoopConfiguration().set("fs.s3a.region", os.environ["us-west-2"])

import os
import errno
import time
import shutil
import pandas
import pywtk.wtk_api as pwtk
import pywtk
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.utils import require_minimum_pandas_version, require_minimum_pyarrow_version
from spark_utils import create_spark_session, write_to_postgres
from wtk_utils import get_wtk_output_uri

import boto3
from botocore.exceptions import NoCredentialsError

# Set attributes you want
attributes = ['density', 'power', 'pressure', 'temperature', 'wind_direction', 'wind_speed']

# List of years 2007-2015
start = pandas.Timestamp('2007-01-01', tz='utc')
end = pandas.Timestamp('2015-01-01', tz='utc')

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except OSError as e:
        if e.errno == errno.ENOENT:
            return False
        else:
            raise
    except NoCredentialsError:
        print("Credentials not available")
        return False

uploaded = upload_to_aws('local_file', 'bucket_name', 's3_file_name')

class DownloadSite:
    def __init__ (self, site_id):
        self.site_id = site_id

    def get_pandas(self, id):
        site_data = pwtk.get_wind_data(id, start, end, attributes=attributes, source='nc')
        return site_data

    def get_parquet(self, site_id, pd):
        #Change the timestamp index to a column
        pd = pd.reset_index()

        # Write pandas to parquet
        pd.to_parquet("%s.parquet"%site_id, engine='pyarrow')

    def write_parquet_to_s3(self):
        #Write to the S3 bucket
        local_file = "%s.parquet" % self.site_id
        #bucket = get_wtk_output_uri()
        bucket = "windtoolkit"
        s3_file = "pywtk-data/%s" % local_file
        print("Localfile: %s S3 => %s " % (local_file, bucket))
        uploaded = upload_to_aws(local_file, bucket, s3_file)
        print("Localfile: %s S3 => %s (%s)" % (local_file, bucket, uploaded))
        # TODO(): Retry
        os.remove(local_file)

    def download_site(self):
        print("DOWNLOADING SITE ===> %s" % self.site_id)
        site_pd = self.get_pandas(self.site_id)
        site_pq = self.get_parquet(self.site_id, site_pd)
        self.write_parquet_to_s3()

def site_downloader(site_id):
    #print("DOWNLOADING SITE ===> %s" % site_id)
    downloader = DownloadSite(site_id)
    downloader.download_site()

if __name__ == '__main__':
    require_minimum_pandas_version()
    require_minimum_pyarrow_version()

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
    dist_site_list.foreach(lambda site: site_downloader(site))

    dist_site_list.collect()

