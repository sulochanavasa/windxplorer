import sys
import os
from sqlalchemy import create_engine, inspect

# DB Tables
SITE_INFO="site_info"
SITE_TOTAL_DAILY="site_total_daily_capacity"
SITE_TOTAL_MONTHLY="site_total_monthly_capacity"
SITE_TOTAL_YEARLY="site_total_yearly_capacity"
SITE_SCORE="site_score"
SITE_WIND_POWER="site_tot_power_wind_dir"
SITE_AVG_DAILY="site_avg_daily_capacity"
SITE_AVG_MONTHLY="site_avg_monthly_capacity"
SITE_AVG_YEARLY="site_avg_yearly_capacity"

# Read database configuration from environment variables. If not specified, use the default config.
DB_HOST = os.getenv('POSTGRES_HOST', '10.0.0.10')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PWD = os.getenv('POSTGRES_PWD', 'postgres')

def postgres_init():
    db_string = f'postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/windexplorer'

    # Create db connection
    db = create_engine(db_string)

    return db

DB = postgres_init()

def get_site_info_db():
    result_set = DB.execute(f'SELECT * FROM {SITE_INFO}, {SITE_SCORE} WHERE {SITE_INFO}.site_id={SITE_SCORE}.site_id')

    return [dict(site) for site in result_set]

def get_all_sites():
    result_set = DB.execute(f'SELECT * FROM {SITE_INFO}')

    return [dict(site) for site in result_set]

def get_site_avg_yearly(site_id):
    result_set = DB.execute(f'SELECT site_id, year, avgwindspeed FROM {SITE_AVG_YEARLY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_avg_monthly(site_id):
    result_set = DB.execute(f'SELECT site_id, year, month, avgwindspeed FROM {SITE_AVG_MONTHLY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_avg_daily(site_id):
    result_set = DB.execute(f'SELECT site_id, year, month, day, avgwindspeed FROM {SITE_AVG_DAILY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_wind_power(site_id):
    result_set = DB.execute(f'SELECT * FROM {SITE_WIND_POWER} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_total_daily_capacity(site_id):
    result_set = DB.execute(f'SELECT * FROM {SITE_TOTAL_DAILY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_total_monthly_capacity(site_id):
    result_set = DB.execute(f'SELECT * FROM {SITE_TOTAL_MONTHLY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

def get_site_total_yearly_capacity(site_id):
    result_set = DB.execute(f'SELECT * FROM {SITE_TOTAL_YEARLY} WHERE site_id={site_id}')

    return [dict(site) for site in result_set]

if __name__ == "__main__":
    db = postgres_init()
    site_info_rows = get_site_info_db()
    
    #print(site_info_rows[0])
    #print(get_site_total_daily_capacity(11021))
    #print(get_site_total_monthly_capacity(11021))
    #print(get_site_total_yearly_capacity(11021))
    #print(len(set(get_partial_sites())))
    #print(get_site_wind_power(11021))
    #print(get_site_avg_daily(11021))
    #print(get_site_avg_monthly(11021))
    #print(get_site_avg_yearly(11021))

