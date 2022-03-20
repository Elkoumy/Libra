"""In this file, we build the shell jobs to run on the slurm HPC"""
import os
import subprocess
import time
from Libra import anonymize_event_log
dir_path = os.path.dirname(os.path.realpath(__file__))
jobs_dir = "jobs"




""" A  time  limit  of  zero  requests  that no time limit be imposed.  Acceptable time
              formats    include    "minutes",    "minutes:seconds",     "hours:minutes:seconds",
              "days-hours", "days-hours:minutes" and "days-hours:minutes:seconds".
              """

# datasets = ["CCC19_t",  "Unrineweginfectie_t", "Sepsis_t","Traffic_t", "Hospital_t", "CreditReq_t", "BPIC15_t","BPIC20_t", "BPIC13_t",
# "BPIC12_t", "BPIC17_t", "BPIC14_t", "BPIC19_t", "BPIC18_t" ]



# datasets = ["CCC19_t",  "Unrineweginfectie_t", "Traffic_t", "Hospital_t", "CreditReq_t", "BPIC15_t","BPIC20_t", "BPIC13_t",
# "BPIC12_t", "BPIC17_t", "BPIC14_t", "BPIC19_t", "BPIC18_t" ]

datasets = ["CCC19_t",  "Unrineweginfectie_t", "Sepsis_t","Traffic_t", "Hospital_t", "CreditReq_t", "BPIC15_t","BPIC20_t", "BPIC13_t",
"BPIC12_t", "BPIC17_t", "BPIC14_t", "BPIC19_t" ]

datasets=["BPIC18_t"]


epsilons = [0.05,0.75,2]
b=2
memory = 4
exec_time="01:00:00" # 1 hour

# no_of_iterations =14
# start_iteration=13

no_of_iterations =1
start_iteration=0

gamma = 0.05
epsilon_in_minutes = 20
alpha = 5
delta = 1e-4

alphas=[2,10,100]



for data in datasets:

    for alpha in alphas:
        anonymize_event_log(data,b,gamma,alpha,epsilon_in_minutes, delta)

