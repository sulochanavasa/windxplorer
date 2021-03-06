
WTK_DIR="windtoolkit"
WTK_DATA_DIR="/home/ubuntu/data/met_data"
SITE_TIMEZONE_CSV="site_timezone.csv"
SITE_METADATA_CSV="three_tier_site_metadata.csv"
SITE_GEODATA_CSV="site_geo_data.csv"

WTK_OUTPUT_DIR="pywtk-data"

def get_timezone_uri():
    return "s3a://%s/%s" % (WTK_DIR, SITE_TIMEZONE_CSV)

def get_metadata_uri():
    return "s3a://%s/%s" % (WTK_DIR, SITE_METADATA_CSV)

def get_geodata_uri():
    return "s3a://%s/%s" % (WTK_DIR, SITE_GEODATA_CSV)

def get_wtk_output_uri():
    return "s3://%s/%s" % (WTK_DIR, WTK_OUTPUT_DIR)

def get_parquet_uri(site_id):
    return "s3a://%s/%s/%d.parquet" % (WTK_DIR, WTK_OUTPUT_DIR, site_id)

def get_nc_file_path(site_id):
    dir_ = str((int(site_id)/500))
    return "%s/%s/%s.nc" % (WTK_DATA_DIR, dir_, site_id)

def aws_copy_files(file_name, sub_dir):
    import boto3

    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': 'windtoolkit',
        'Key': "pywtk-data/%s" % file_name
    }
    print("COPY pywtk-data/%s to pywtk-data/%s/%s" % (file_name, sub_dir, file_name))
    s3.meta.client.copy(copy_source, 'windtoolkit',
                                     "pywtk-data/%s/%s" % (sub_dir, file_name))

if __name__ == '__main__':
    print(get_timezone_uri())
    print(get_metadata_uri())
    print(get_geodata_uri())
    print(get_wtk_output_uri())
    print(get_parquet_uri(1000))
    aws_copy_files("0.parquet", "RUN1")
