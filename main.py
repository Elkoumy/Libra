from event_log_sampling import trace_sampling
from utilities_module import read_event_log
import sys
import warnings
import time
import pandas as pd

from math import sqrt











if __name__ == "__main__":
    if not sys.warnoptions:
        warnings.simplefilter("ignore")

    start_time = time.time()
    # read event log
    log=read_event_log('data/Traffic.csv')
    #todo: 2 layers sampling with Anonymization
    sampled= trace_sampling(log)

    end_time = time.time()
    print("execution time = % seconds" %(str(end_time - start_time)) )

    log = log['case:concept:name'].unique().size
    print("Log size is: %s cases" % (log))

    sample_size= sampled['case:concept:name'].unique().size
    print("Sample size is: %s cases" %(sample_size))
    #todo: performance measures
    print("**")



