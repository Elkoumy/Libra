"""In this file, we build the shell jobs to run on the slurm HPC"""
import os
import subprocess
import time

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

datasets = [  "Unrineweginfectie_t", "Sepsis_t","Traffic_t", "Hospital_t", "CreditReq_t", "BPIC15_t","BPIC20_t", "BPIC13_t",
"BPIC12_t", "BPIC17_t", "BPIC14_t" ]

# datasets=["Traffic_t"]


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
delta = 1e-3

alphas=[2,10,100]



for data in datasets:

    for alpha in alphas:


        for iteration in range(start_iteration, no_of_iterations):
            if data in ["CCC19_t","Unrineweginfectie_t"]:
                memory = 32
                exec_time = "00:5:00"  # 1 minutes
            elif data in ["Traffic_t"]:
                memory=40
            elif data in ["Sepsis_t","CreditReq_t", "BPIC15_t"]:
                memory = 32
                exec_time = "00:20:00"  # 7 minutes
            elif data in [ "Hospital_t"]:
                memory = 32
                exec_time = "01:00:00"  # 25 minutes
            elif data in ["BPIC20_t", "BPIC13_t"]:
                memory = 32
                exec_time = "01:00:00"  # 30 minutes
            elif data in ["BPIC12_t" ]:
                memory = 32
                exec_time = "01:00:00"  # 32 minutes

            elif data in ["BPIC17_t"]:
                memory = 32
                exec_time = "02:40:00"  # 40 minutes
            elif data in ["BPIC14_t"]:
                memory = 32
                exec_time = "03:30:00"  # 1.5 hours

            elif data in [  "BPIC19_t"]:
                # memory = 32
                memory = 36
                exec_time = "24:00:00"  # 4 hours

            elif data in ["BPIC18_t"]:
                memory = 80
                exec_time = "30:00:00"  # 5 hours


            job_name = os.path.join(jobs_dir,"j_%s_%s.sh" % (data, alpha))
            job_log_name =os.path.join(jobs_dir,"l_%s_%s.txt" % (data,alpha))

            with open(job_name, "w") as fout:
                fout.write("#!/bin/bash\n")
                fout.write("#SBATCH --output=jobs/l_%s_%s.txt" % (data,alpha))
                fout.write("#SBATCH --mem=%sGB\n" % memory)
                fout.write("#SBATCH --ntasks=1\n")  ## Run on a single CPU
                #fout.write("#SBATCH --cpus-per-task=12\n")  # 8 cores per cpu
                fout.write("#SBATCH --partition=amd\n")
                fout.write("#SBATCH --time=%s\n" % (exec_time))
                fout.write("module load python/3.8.6\n")
                # fout.write("cd ..\n")

                # dataset = os.sys.argv[1]
                # delta = os.sys.argv[2]
                # precision = os.sys.argv[3]
                # iteration = os.sys.argv[4]
                fout.write("python -u %s \"%s\" %s %s %s %s %s \n"
                           %('"'+os.path.join(dir_path,"Libra.py")+'"',
                                                       data,
                                                       b,
                                                       gamma,
                                                       alpha,
                             epsilon_in_minutes,
                                                       delta))

            time.sleep(1)
            subprocess.Popen(("sbatch %s" % job_name).split())