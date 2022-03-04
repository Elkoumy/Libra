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
    event_log="Traffic"
    log=read_event_log('data/%s.csv'%(event_log))
    #2 layers sampling with Anonymization
    prob=0.1
    eps=1.0
    epsilon_in_minutes=200

    sampled= trace_sampling(log, prob,epsilon_in_minutes)

    sampled['time:timestamp']=sampled['time:timestamp'].astype('datetime64[s]')
    sampled.to_csv("output/anonymized_%s.csv"%(event_log))
    end_time = time.time()
    print("execution time = % seconds" %(str(end_time - start_time)) )

    log = log['case:concept:name'].unique().size
    print("Log size is: %s cases" % (log))

    sample_size= sampled['case:concept:name'].unique().size
    print("Sample size is: %s cases" %(sample_size))
    #todo: performance measures
    print("**")



