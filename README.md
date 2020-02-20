# WindExplorer
This is a project I completed during the Insight Data Engineering program (Silicon Valley, Spring 2020).

It's a platform to help locate potential wind sites that are suitable for wind farm development by providing a site score for its wind capcacity in correlation with the other sites, average daily, daily and monthly data for wind profiles, wind capacity by wind directions and historical trends for each of the wind sites.

I built a batch processing pipeline that ingests 2-TB of techno-economic wind data hosted on an S3 bucket in HDF5 Format. Per site data is extracted from the source files using PYWTK toolkit that returns the wind data in panda data frames. These dataframes are converted to Parquet file format using Apache Arrow and are stored back in S3. Spark read these files from S3, performs necessary computations and stores the data in PostgreSQL database. The resulsts are visualized through Flask on Leaflet, to obtain site relevant information and historical trends through charts.

## Pipeline


## Engineering Challenges
- Data is in NetCDF/HDF5 File Format which is not supported by Spark.
- S3 connectors are not natively available to read from S3.
- Developer APIs are rate limited to access 2TB and 50TB in CSV format.
- File format is not distributed.
- Needed to get creative to query per site information to obtain smaller subsets of information.

## Dataset
- Techo-economic source files by location in HDF5 format \
  https://registry.opendata.aws/nrel-pds-wtk/ \
  arn:aws:s3:::nrel-pds-wtk/wtk-techno-economic/pywtk-data/ 
- Managed by: \
NREL (https://www.nrel.gov/) 
- Documentation: \
  https://www.nrel.gov/grid/wind-toolkit.html \
  2-TB time-series data for 7 years (2007-2013) at 120,000 points within the continental U.S.

## Cluster Structure:


