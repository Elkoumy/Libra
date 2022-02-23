import pandas as pd

import math
from anonymization import draw_anonymized_sample
import random
from utilities_module import get_dfg_time

def calculate_trace_threshold(log):
    alpha=0.01
    delta= 0.05
    z=1-alpha

    threshold=1/(2*delta )* (-2*delta**2 + z**2 +math.sqrt(z))
    print("Traces Threshold :%s" %(threshold))
    return threshold

def trace_sampling(log):
    accummulated=0
    epsilon_in_minutes=20 # is a parameter for their experiment
    log_cases_count = log['case:concept:name'].unique().size
    activity_list=[]
    starting_activity_list=[]
    ending_activity_list=[]
    edge_list=[]
    cycle_time_list=pd.Series()
    edge_time_list= {} #to_dataframe
    cases_without_new_information=0 #counter for the cases without updates

    cases_with_new_information =[] #accumulation of cases to be sampled.

    # calculate the statistical threshold
    threshold = calculate_trace_threshold(log)


    while(cases_without_new_information<threshold):
        anonymized_samples_count=0
        #todo: draw an anonymized sample from the log
        sample =draw_anonymized_sample(log)
        anonymized_samples_count+=1





        """##############################################"""
        #iterate over traces in the log the traces should be picked randomly.
        #We can do that by selecting the trace ids in a list, then picking up randomly from that list.
        case_ids=sample['case:concept:name'].unique().tolist()
        random.shuffle(case_ids)

        for case in case_ids:


            current_case= sample[sample['case:concept:name']==case]
            if current_case.shape[0]==0:
                break
            current_case=current_case.sort_values(by='time:timestamp')
            new_information_gained=False


            #new event
            activities= current_case['concept:name'].unique().tolist()
            for activity in activities:
                if activity not in activity_list:
                    new_information_gained=True
                    activity_list.append(activity)

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
            edges=get_dfg_time(current_case)
            for edge in edges:
                if edge not in edge_list:
                    new_information_gained= True
                    edge_list.append(edge)

            #changes in the average cycle time by epsilon
            #timedelta in hours
            cycle_time=(current_case['time:timestamp'].iloc[-1]- current_case['time:timestamp'].iloc[0])/ pd.Timedelta('1 minute')

            if new_information_gained:
                cycle_time_list=cycle_time_list.append(pd.Series(cycle_time), ignore_index=True)
            else:
                avg_cycle_time=cycle_time_list.sum()/cycle_time_list.size
                avg_new=(cycle_time_list.sum()+cycle_time)/(cycle_time_list.size+1)
                #todo: make the cycle time change to be epsilon
                diff = math.fabs(avg_new-avg_cycle_time)
                if diff >epsilon_in_minutes:
                    new_information_gained = True
                    cycle_time_list=cycle_time_list.append(pd.Series(cycle_time), ignore_index=True)


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

            # update counters
            if not new_information_gained:
                cases_without_new_information+=1
                cases_with_new_information.append(case)
            else:
                cases_without_new_information=0

            #break if number of cases reaches threshold
            if cases_without_new_information>=threshold:
                break

        """End of for over the cases in the current sample"""
        ######################
    print("end of while loop")
    """End of while loop"""
    """#################"""
    if len(cases_with_new_information) < log_cases_count:
        sample=log[log['case:concept:name'].isin(cases_with_new_information)]
        return sample


    return log