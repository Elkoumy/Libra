import sys
import warnings
import os
import time
from event_log_sampling import main_anonymization
from utilities_module import read_event_log_xes
from pm4py.objects.conversion.log import factory as conversion_factory
from pm4py.objects.log.exporter.xes import factory as xes_exporter


def anonymize_event_log(event_log, eps=2, gamma=0.05, alpha=5,epsilon_in_minutes=20,delta=1e-4):
    start_time = time.time()

    log = read_event_log_xes('data/%s.xes' % (event_log))

    sampled, eps_after_composition = main_anonymization(log, gamma, eps, epsilon_in_minutes, alpha, delta)
    if not type(sampled) == type(0):
        sampled['time:timestamp'] = sampled['time:timestamp'].astype('datetime64[s]')

        print("The amplified eps %s ---> Original eps = %s " % (str(round(eps_after_composition, 2)), eps))
        log = log['case:concept:name'].unique().size
        print("Log size is: %s cases" % (log))

        sample_size = sampled['case:concept:name'].unique().size
        print("Sample size is: %s cases" % (sample_size))

        #export XES file
        log = conversion_factory.apply(sampled)
        xes_exporter.export_log(log, "output/%s_eps%s.xes" % (event_log, round(eps_after_composition, 2)))

        # sampled.to_csv("output/%s_eps%s.csv"%(event_log,round(eps_after_composition,2)))
        end_time = time.time()
        print("execution time = % seconds" % (str(end_time - start_time)))


if __name__ == "__main__":
    if not sys.warnoptions:
        warnings.simplefilter("ignore")

    event_log=os.sys.argv[1]
    eps = float(os.sys.argv[2])

    gamma = float(os.sys.argv[3])
    alpha = int(os.sys.argv[4])
    epsilon_in_minutes = float(os.sys.argv[5])
    delta = float(os.sys.argv[6])

    anonymize_event_log(event_log,eps,gamma,alpha,epsilon_in_minutes,delta)