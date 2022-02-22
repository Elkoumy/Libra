from event_log_sampling import trace_sampling
from utilities_module import read_event_log
import sys
import warnings
import pandas as pd














if __name__ == "__main__":
    if not sys.warnoptions:
        warnings.simplefilter("ignore")


    #TODO: read event log
    log=read_event_log('data/Sepsis.csv')
    #todo: 2 layers sampling with Anonymization
    sampled= trace_sampling(log)
    #todo: performance measures
    print("**")


