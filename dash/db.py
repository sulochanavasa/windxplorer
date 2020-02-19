import sys
import os
from sqlalchemy import create_engine, inspect

# DB Tables
SITE_INFO="site_info"
SITE_TOTAL_DAILY="site_total_daily_capacity"
SITE_TOTAL_MONTHLY="site_total_monthly_capacity"
SITE_TOTAL_YEARLY="site_total_yearly_capacity"
SITE_SCORE="site_score"

# Read database configuration from environment variables. If not specified, use the default config.
DB_HOST = os.getenv('POSTGRES_HOST', '10.0.0.6')
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

# Temporary API to get only the sites we have
def get_partial_sites():
    result_set = DB.execute(f'SELECT site_id FROM {SITE_TOTAL_DAILY}')

    return [dict(site)['site_id'] for site in result_set]

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
    #print(get_site_total_daily_capacity(21121))
    #print(get_site_total_monthly_capacity(21121))
    #print(get_site_total_yearly_capacity(21121))
    #print(get_partial_sites())
    print(get_site_score()[0])