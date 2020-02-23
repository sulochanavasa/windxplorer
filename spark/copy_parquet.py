import os
import sys
import pandas
import itertools
import threading
import time
sys.path.append("/home/ubuntu/programs/windexplorer/dash")

from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql import types
from wtk_utils import aws_copy_files
from db import get_all_sites

# 5 cores per executor good practice
# Total Cores = 4*3 = 12
# Total executors = 10/5 = 2. So, 1 executor per node
# Memory per node per executor.

SITES_PER_JOB = 5000

def move_parquet(site_list, run_id):
    for site in site_list:
        print(site)
        aws_copy_files("%d.parquet"%site, "RUN%s" %run_id)

 # Print the number of sites per state in this slice
def print_state_count(site_list, site):
    state_dict = {}
    for s in site_list:
        state = site[s]['state']
        if state not in state_dict:
            state_dict[state] = 1
        else:
            state_dict[state] = state_dict[state] + 1
    for s in state_dict:
        print("STATE %s => %u" % (s, state_dict[s]))
    print("NUMBER OF STATES => %u" % len(state_dict))

def all_lists_empty(lists):
    for l in lists:
        if len(l):
            return False
    return True

 # Pick from each state and build a list
def evenly_spaced(site_list_of_lists):
    l = []
    while not all_lists_empty(site_list_of_lists):
        for site_list in site_list_of_lists:
            if len(site_list):
                l.append(site_list.pop(len(site_list)-1))
    return l

def spark_split_jobs():
    # List of states and sites
    state = {}
    site = {}
    db = get_all_sites()
    print("TOTAL SITES %u" % len(db))
    for site_info in db:
        # Accumulate by state
        if not site_info['state'] in state:
            state[site_info['state']] = [site_info['site_id']]
        else:
            state[site_info['state']].append(site_info['site_id'])

        # Reverse index by site_id
        if not site_info['site_id'] in site:
            site[site_info['site_id']] = site_info

    site_list_of_lists = []
    for s in state:
        site_list_of_lists.append(state[s])

    # Distributed the states evenly in the list
    zipped_list = evenly_spaced(site_list_of_lists)

    run_id = 2
    split_list = zipped_list[5000:10000]
    move_parquet(split_list, run_id)
    run_id = run_id + 1

    threads = []

    for i in range(0, len(zipped_list), SITES_PER_JOB):
        if i < 2:
            continue
        split_list = zipped_list[i:i+SITES_PER_JOB]
        #print_state_count(split_list, site)
        # Move parquet files to RUNx folder
        t = threading.Thread(target = move_parquet, args = (split_list, run_id))
        t.start()
        time.sleep(1)
        threads.append(t)
        #move_parquet(split_list, run_id)
        # Run the spark job on the RUNx folder files
        run_id = run_id + 1

    for t in threads:
        t.join()
    print("ALL FILES COPIED!")


if __name__ == "__main__":
     spark_split_jobs()
