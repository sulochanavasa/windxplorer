
WTK_DIR="windtoolkit"
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
    return "s3a://%s/%s" % (WTK_DIR, WTK_OUTPUT_DIR)

if __name__ == '__main__':
    print(get_timezone_uri())
    print(get_metadata_uri())
    print(get_geodata_uri())
    print(get_wtk_output_uri())
