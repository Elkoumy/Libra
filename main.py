from event_log_sampling import main_anonymization
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
    event_log="Sepsis"
    epsilon_in_minutes = 200
    c=3 #clipping
    log=read_event_log('data/%s.csv'%(event_log))
    #2 layers sampling with Anonymization
    # for gamma in [0.01,0.05,0.1,0.2]:
    #     for eps in [0.01,0.05,0.1,1.0]:
    #         for alpha in [2,10,100,1000]:
    #             try:
    #                 event_log = "Sepsis"
    #                 epsilon_in_minutes = 200
    #                 log = read_event_log('data/%s.csv' % (event_log))
    #                 print("Running Experiment alpha:%s,\t gamma:%s,\t eps:%s" %(alpha,gamma,eps))
    #                 sampled, eps_after_composition = main_anonymization(log, gamma, eps, epsilon_in_minutes, alpha,c)
    # 
    #                 sampled['time:timestamp'] = sampled['time:timestamp'].astype('datetime64[s]')
    #                 sampled.to_csv("output/%s_e%s_eps%s_g%s_a%s.csv" %(event_log,eps, str(round(eps_after_composition, 2)), gamma, alpha))
    #                 end_time = time.time()
    #                 print("execution time = % seconds" % (str(end_time - start_time)))
    # 
    #                 print("The amplified eps %s ---> Original eps = %s " %(str(round(eps_after_composition, 2)), eps))
    #                 log = log['case:concept:name'].unique().size
    #                 print("Log size is: %s cases" % (log))
    # 
    #                 sample_size = sampled['case:concept:name'].unique().size
    #                 # print("Sample size is: %s cases" % (sample_size))
    #                 # todo: performance measures
    #                 # print("**")
    #             except:
    #                 print("Error")

    gamma=0.1
    eps=0.1
    epsilon_in_minutes=200
    alpha=10

    sampled, eps_after_composition= main_anonymization(log, gamma,eps,epsilon_in_minutes,alpha,c)

    sampled['time:timestamp']=sampled['time:timestamp'].astype('datetime64[s]')
    sampled.to_csv("output/%s_eps%s_g%s_a%s.csv"%(event_log,eps_after_composition, gamma, alpha))
    end_time = time.time()
    print("execution time = % seconds" %(str(end_time - start_time)) )

    print("The amplified eps %s ---> Original eps = %s " %(str(round(eps_after_composition,2)), eps))
    log = log['case:concept:name'].unique().size
    print("Log size is: %s cases" % (log))

    sample_size= sampled['case:concept:name'].unique().size
    print("Sample size is: %s cases" %(sample_size))
    #todo: performance measures
    print("**")



