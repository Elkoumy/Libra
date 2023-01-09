from event_log_sampling import main_anonymization
from utilities_module import read_event_log_xes
import sys
import warnings
import time
from pm4py.objects.conversion.log import factory as conversion_factory
from pm4py.objects.log.exporter.xes import factory as xes_exporter

import pandas as pd

from math import sqrt











if __name__ == "__main__":
    if not sys.warnoptions:
        warnings.simplefilter("ignore")

    start_time = time.time()
    # read event log
    event_log="CreditReq_t"

    # log=read_event_log('data/%s.csv'%(event_log))

    #read XES file
    log=read_event_log_xes('data/%s.xes'%(event_log))

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

    gamma=0.05
    b=2
    epsilon_in_minutes=20
    alpha=5
    delta=1e-3

    sampled, eps_after_composition= main_anonymization(log, gamma,b,epsilon_in_minutes,alpha,delta)
    if not type(sampled)==type(0):
        sampled['time:timestamp']=sampled['time:timestamp'].astype('datetime64[s]')


        print("The amplified eps %s ---> Original eps = %s " % (str(round(eps_after_composition, 2)), b))
        log = log['case:concept:name'].unique().size
        print("Log size is: %s cases" % (log))

        sample_size = sampled['case:concept:name'].unique().size
        print("Sample size is: %s cases" % (sample_size))

        #todo: export XES file
        log = conversion_factory.apply(sampled)
        xes_exporter.export_log(log, "output/%s_eps%s.xes"%(event_log,round(eps_after_composition,2)))

        # sampled.to_csv("output/%s_eps%s.csv"%(event_log,round(eps_after_composition,2)))
        end_time = time.time()
        print("execution time = % seconds" %(str(end_time - start_time)) )







