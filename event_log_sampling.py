import pandas as pd

import math
from anonymization import draw_anonymized_sample, privacy_accountant
import random
from utilities_module import get_dfg_time
from anonymization import clip_trace_variants, clip_rare_traces

import time

def calculate_trace_threshold(log):
    alpha=0.01
    delta= 0.05
    z=1-alpha

    threshold=1/(2*delta )* (-2*delta**2 + z**2 +math.sqrt(z))
    # print("Traces Threshold :%s" %(threshold))
    return threshold

def main_anonymization(log,gamma=0.15, eps=1.0,epsilon_in_minutes=10, alpha=2,delta=1e-4):

    result=[]

    #Todo: Clip trace variants < C
    log = clip_rare_traces(log,eps,delta)

    if log.shape[0]==0:
        print("Empty result")
        return 0,0

    #todo: loop to finish N/L

    iterations=round(1/gamma)
    # print(iterations)
    # iterations = 3

    for i in range(0,iterations):
        # print("Sample Iteration: %s"%(i))
        # epsilon_in_minutes=10 # is a parameter for their experiment
        # draw an anonymized sample from the log
        sample = draw_anonymized_sample(log, gamma, eps)
        if sample.shape[0]>0:
                sample=sample[['case:concept:name', 'concept:name','time:timestamp','org_timestamp', 'start_event']]
                sample['case:concept:name']= sample['case:concept:name']+str(i)
                sample=per_lot_sampling(sample,epsilon_in_minutes)
                result.append(sample)
    result=pd.concat(result,ignore_index=True)


    eps_after_composition= privacy_accountant(eps,gamma,iterations, alpha)

    return result, eps_after_composition



def per_lot_sampling(sample, epsilon_in_minutes):
    activity_list=[]
    starting_activity_list=[]
    ending_activity_list=[]
    edge_list=[]
    cycle_time_list=pd.Series()
    edge_time_list= {} #to_dataframe
    cases_without_new_information=0 #counter for the cases without updates

    cases_with_new_information =[] #accumulation of cases to be sampled.
    sample_cases_count = sample['case:concept:name'].unique().size
    # calculate the statistical threshold
    threshold = calculate_trace_threshold(sample)
    total_get_dfg_time=0
    total_edge_average_time_loop=0
    total_loop_over_cases=0
    total_new_edge_loop=0
    total_new_event_loop=0
    part_time=0

    total_number_of_iterations_over_cases=0

    # while(cases_without_new_information<threshold and total_number_of_iterations_over_cases<log_cases_count):
    # print("log_cases_count %s"%(log_cases_count))
    # print("total_number_of_iterations_over_cases %s"%(total_number_of_iterations_over_cases))

    anonymized_samples_count=0




    anonymized_samples_count+=1

    sample_dfg=get_dfg_time(sample)

    sample['case_id']=sample['case:concept:name']
    sample = sample.set_index(['case_id'])

    """##############################################"""
    #iterate over traces in the log the traces should be picked randomly.
    #We can do that by selecting the trace ids in a list, then picking up randomly from that list.
    case_ids=sample['case:concept:name'].unique().tolist()
    # print("len of case_ids %s"%(len(case_ids)))

    random.shuffle(case_ids)

    iteration=0
    # print("starting loop over cases")
    loop_start_time= time.time()
    for case in case_ids:
        # print("iteration %s" %(iteration))
        iteration+=1
        total_number_of_iterations_over_cases+=1

        part_start_time=time.time()
        current_case= sample.loc[case]
        if current_case.shape[0]==0:
            break
        # current_case=current_case.sort_values(by='time:timestamp')
        part_end_time = time.time()
        part_time += part_end_time - part_start_time
        new_information_gained=False
        #new event
        start_time=time.time()
        activities= current_case['concept:name'].unique().tolist()
        for activity in activities:
            if activity not in activity_list:
                new_information_gained=True
                activity_list.append(activity)

        end_time = time.time()
        total_new_event_loop+=end_time-start_time
        # print("loop over unique activities: %s Seconds" % (str(end_time - start_time)))

        #new starting event
        start_activity=current_case['concept:name'].iloc[0]
        if start_activity not in starting_activity_list:
            new_information_gained = True
            starting_activity_list.append(start_activity)

        #new ending event
        end_activity = current_case['concept:name'].iloc[-1]
        if end_activity not in ending_activity_list:
            new_information_gained = True
            ending_activity_list.append(end_activity)


        #new edge
        new_edge=False
        start_time=time.time()

        edges=sample_dfg.loc[case].to_dict()['difference']
        # edges=edges.groupby(['concept:name', 'concept:name_2']).difference.sum().to_dict()
        end_time = time.time()
        total_get_dfg_time+=end_time-start_time
        # print("get dfg time: %s Seconds" % (str(end_time - start_time)))

        start_time=time.time()
        for edge in edges:
            if edge not in edge_list:
                new_information_gained= True
                edge_list.append(edge)
        end_time = time.time()
        total_new_edge_loop+=end_time-start_time
        # print("new edge loop : %s Seconds" % (str(end_time - start_time)))
        #changes in the average cycle time by epsilon
        #timedelta in hours

        cycle_time=(current_case['time:timestamp'].iloc[-1]- current_case['time:timestamp'].iloc[0])/ pd.Timedelta('1 minute')


        if new_information_gained:
            cycle_time_list=cycle_time_list.append(pd.Series(cycle_time), ignore_index=True)
        else:

            #todo: enhance the following code block
            avg_cycle_time=cycle_time_list.sum()/cycle_time_list.size
            avg_new=(cycle_time_list.sum()+cycle_time)/(cycle_time_list.size+1)
            #make the cycle time change to be epsilon
            diff = math.fabs(avg_new-avg_cycle_time)
            if diff >epsilon_in_minutes:
                new_information_gained = True
                cycle_time_list=cycle_time_list.append(pd.Series(cycle_time), ignore_index=True)

        start_time=time.time()
        #changes average time on edge by epsilon
        for edge in list(edges.keys()):
            if edge in edge_time_list:
                #compare
                avg_edge_time=sum(edge_time_list[edge])/len(edge_time_list[edge])
                new_avg_edge_time = (sum(edge_time_list[edge])+ edges[edge])/ (len(edge_time_list[edge])+1)

                if avg_edge_time >0:
                    diff = math.fabs(avg_edge_time - new_avg_edge_time)
                    # make the edge time change to be epsilon
                    if diff > epsilon_in_minutes:
                        new_information_gained = True
                        edge_time_list[edge].append(edges[edge])
            else:
                new_information_gained = True
                edge_time_list[edge]=[edges[edge]]
        end_time=time.time()
        total_edge_average_time_loop+=end_time-start_time
        # print("edge average time loop: %s Seconds" %(str(end_time - start_time)))

        # update counters
        if not new_information_gained:
            cases_without_new_information+=1

        else:
            cases_without_new_information=0
            cases_with_new_information.append(case)

        #break if number of cases reaches threshold
        if cases_without_new_information>=threshold:
            # print("cases_without_new_information>=threshold")
            break

        if total_number_of_iterations_over_cases >= sample_cases_count:
            return sample



    loop_end_time=time.time()
    total_loop_over_cases+=loop_end_time-loop_start_time
    # print("loop over cases: %s Seconds" % (str(loop_end_time - loop_start_time)))
    """End of for over the cases in the current sample"""
    ######################

    # if cases_without_new_information >= threshold:
    #     break

    # print("end of while loop")
    # """End of while loop"""
    """#################"""

    # print("total_get_dfg_time %s"%(total_get_dfg_time))
    # print("total_edge_average_time_loop %s" %(total_edge_average_time_loop))
    # print("total_loop_over_cases %s"%(total_loop_over_cases))
    # print("total_new_edge_loop %s"%(total_new_edge_loop))
    # print("total_new_event_loop %s"%(total_new_event_loop))
    # print("total part time %s"%(part_time))

    if len(cases_with_new_information) < sample_cases_count:
        res=sample[sample['case:concept:name'].isin(cases_with_new_information)]
        return res


    return sample