"""
This module implements the functionality of reading the input from the user. The following formats are supported:
    * DFG
    * XES
"""
import pandas as pd
import time
import numpy as np
# from amun.trie import PrefixTree
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.statistics.traces.log.case_statistics import get_variant_statistics
from pm4py.objects.conversion.log.versions.to_dataframe import get_dataframe_from_event_stream
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.dfg import factory as dfg_factory

# from amun.edges_pruning import pruning_by_edge_name_freq, pruning_by_edge_name_time
# from amun.guessing_advantage import AggregateType
from math import log10
import os
# from amun.dafsa_classes import DAFSA
# import swifter
# from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.filtering.log.start_activities import start_activities_filter
from pm4py.algo.filtering.log.end_activities import end_activities_filter
from pm4py.objects.log.importer.csv import factory as csv_importer
from pm4py.objects.conversion.log import factory as conversion_factory
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from pm4py.algo.filtering.log.variants import variants_filter
from pm4py.statistics.traces.log import case_statistics
# from pruning_edges import get_pruning_edges



def xes_to_DAFSA(data_dir):
    """
     This function takes the XES file and returns:
        * DAFSA automata.
        * Event log as a dataframe annotated with states and contains the relative time.
    """

    log = xes_import_factory.apply(data_dir)
    data = get_dataframe_from_event_stream(log)



    log = conversion_factory.apply(data)


    variants_count = case_statistics.get_variant_statistics(log)

    ### Calculate relative time

    data = get_relative_time(data)


    """
         The focus of the following analysis is for the three columns:
         * case:concept:name
         * time:timestamp
         * concept:name

         We keep only the above three column in order to reduce the memory usage
        """
    data = data[['case:concept:name', 'concept:name', 'time:timestamp','relative_time','start_event']]



    data, trace_variants = annotate_eventlog_with_trace_variants(data, log)


    ### Calculate DAFSA

    # dafsa_log=get_DAFSA(log) # we pass the data as it is filtered from the lifecycle

    ### Anotate Event Log with DAFSA states
    # data=annotate_eventlog_with_states(data,log)
    dataset= os.path.split(data_dir)[1][:-4]
    data = annotate_eventlog_with_state_vectorized(data, dataset, trace_variants)

    data.prev_state= data.prev_state.astype(int)
    data.state = data.state.astype(int)

    # if aggregate_type==AggregateType.FREQ:
    #     dfg=dfg_factory.apply(log,variant="frequency")
    # else:
    #     dfg = get_dfg_time(data, aggregate_type, dataset)

    # dafsa_edges,edges_df=get_edges(dafsa_log)
    # return data,dafsa_log,dafsa_edges, edges_df, trace_variants

    return data, variants_count




def annotate_eventlog_with_trace_variants(data, log):

    variants = variants_filter.get_variants(log)

    case_variant_link=[]
    trace_variant_index=list(variants.keys())
    for idx, key in enumerate(trace_variant_index):

       for trace in variants[key]:
           case_variant_link.append([trace.attributes["concept:name"],idx])

    case_variant_link=pd.DataFrame(case_variant_link,columns=['case:concept:name','trace_variant'])

    data = pd.merge(left=data, right=case_variant_link, left_on='case:concept:name', right_on='case:concept:name')
    # data= data.join(case_variant_link, on='case:concept:name', lsuffix='', rsuffix='_linker')
    # data.drop('case:concept:name_linker',inplace=True,axis=1)
    return data, trace_variant_index

def get_edges(dafsa):
    edges=[]
    edges_df=[]
    edge_idx=0 #location within the lookup of edges (edges)
    for idx in dafsa.nodes:
        node=dafsa.nodes[idx]

        for edg_name in node.edges:
            #node.edges[edg_name].activity_name=edg_name
            node.edges[edg_name].state_id=idx
            node.edges[edg_name].lookup_idx=edge_idx
            node.edges[edg_name].activity_name=edg_name

            #some lookup index are taken from the lookup nodes
            dafsa.lookup_nodes[idx].edges[edg_name].state_id=idx
            dafsa.lookup_nodes[idx].edges[edg_name].lookup_idx=edge_idx
            dafsa.lookup_nodes[idx].edges[edg_name].activity_name=edg_name

            edges.append(node.edges[edg_name])

            edges_df.append([edge_idx,node.edges[edg_name].added_noise])
            edge_idx+=1

    edges_df=pd.DataFrame(edges_df,columns=['idx','added_noise'])
    return edges,edges_df

def get_relative_time(data):
    """
    Returns the event log with the relative time difference of every activity
    """
    # taking only the complete event to avoid ambiguoutiy
    # if dataset not in ["BPIC13","BPIC20","BPIC19","BPIC14","Unrineweginfectie","temp"] and dataset[-2:]!='_t':
    #     data=data.where((data["lifecycle:transition"].str.upper()=="COMPLETE" ) )
    #     data=data.dropna(subset=['lifecycle:transition'])

    #moving first row to the last one
    temp_row= data.iloc[0]
    data2=data.copy()
    # data2.drop(data2.index[0], inplace=True)
    # data2=data2.append(temp_row)
    # data2=pd.concat([pd.DataFrame(temp_row), data2], axis=1 ,ignore_index=True)
    # data2.reset_index(inplace=True)
    data2.loc[-1]=temp_row
    data2.index = data2.index + 1  # shifting index
    data2.sort_index(inplace=True)

    #changing column names
    columns= data2.columns
    columns= [i+"_2" for i in columns]
    data2.columns=columns

    #combining the two dataframes into one
    data = data[['case:concept:name', 'concept:name', 'time:timestamp']]
    data2 = data2[['case:concept:name_2', 'concept:name_2', 'time:timestamp_2']]

    data = data.reset_index()
    data2=data2.reset_index()
    data=pd.concat([data, data2], axis=1)



    #calculating time difference
    data['time:timestamp']=pd.to_datetime(data['time:timestamp'],utc=True)
    data['time:timestamp_2'] = pd.to_datetime(data['time:timestamp_2'],utc=True)
    data['start_event'] = False

    data['relative_time'] = (data['time:timestamp'] - data['time:timestamp_2']).astype(
        'timedelta64[m]')   # in  minutes


    # data['relative_time'] = (data['time:timestamp'] - data['time:timestamp_2'])/ np.timedelta64(1, 'D')/30
    # data['relative_time'] = (data['time:timestamp'] - data['time:timestamp_2'])/ np.timedelta64(1, 'h')

    ''' In case of the first activity, we set the relative time to the number of days since the start of the log
        to make it an integer. We handle that in the file epsilon_estimation_start_timestamp.py      
    '''


    min_timestamp= data['time:timestamp'].min()
    data.loc[0,'relative_time']= (data.loc[0]['time:timestamp'] - min_timestamp).components.days/7

    data.loc[data['case:concept:name'] != data['case:concept:name_2'], 'relative_time'] = \
        (data.loc[data['case:concept:name'] !=data['case:concept:name_2'], 'time:timestamp'] - min_timestamp).dt.days/7

    data.loc[data['case:concept:name'] != data['case:concept:name_2'], 'start_event'] = True
    data.loc[0, 'start_event'] = True

    #delete the last row as it is meaningless because data2 is longer by 1
    data.drop(data.tail(1).index, inplace=True)

    data=data[['case:concept:name','concept:name','time:timestamp','relative_time','start_event']]

    return data

def get_DAFSA(log,activity_dict):
    result = get_variant_statistics(log)
    traces = []
    for trace in result:
        current = trace['variant']  # separated by commas ','
        current=current.split(',') #list of activities
        current=[str(activity_dict[i]) for i in current]
        current=','.join(current)
        #replace activities with numbers

        # current=current.replace(',',';')
        traces.append(current)

    # d= DAFSA(["tap", "taps", "top", "tops", "dibs"])

    dafsa_log = DAFSA(traces, delimiter=',')

    return dafsa_log


def annotate_eventlog_with_states(data,log):

    # Replacing activity names with numbers
    activity_names = data['concept:name'].unique()
    nums= [str(i) for i in list(range(len(activity_names)))]
    activity_dict = dict(zip(activity_names, nums ))
    data['concept:name'] = data['concept:name'].replace(activity_dict)

    start = time.time()
    dafsa_log = get_DAFSA(log,activity_dict)
    end = time.time()
    print("building dafsa %s" % (end - start))

    start = time.time()
    states = [] # holds the end state of the dafsa edge
    prev_states=[] #holds the start state of the dafsa edge
    prev_state = 0
    curr_trace = -1
    curr_state=-1
    for idx, row in data.iterrows():
        if curr_trace != row['case:concept:name']:
            prev_state = 0
            curr_trace = row['case:concept:name']

        else:
            prev_state = curr_state
        temp=dafsa_log.nodes[prev_state]


        curr_state = dafsa_log.nodes[prev_state].edges[row['concept:name']].node.node_id

        states.append(curr_state)
        prev_states.append(dafsa_log.nodes[prev_state].node_id)

    data['state'] = states
    data['prev_state']=prev_states
    data.state = data.state.astype(int)
    data.prev_state = data.prev_state.astype(int)
    end = time.time()
    print("sequential dafsa annotation %s" % (end - start))

    #remaping the original activity names
    activity_dict = dict(zip(activity_dict.values(), activity_dict.keys() ))
    data['concept:name'] = data['concept:name'].replace(activity_dict)
    return data

def get_DAFSA_Dictionary(dataset):
    # dataset = "temp"
    # data_dir = r"C:\Gamal Elkoumy\PhD\OneDrive - Tartu Ülikool\Differential Privacy\amun\data"

    #call the jar file and send the parameters to it
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root_dir=os.path.join(cur_dir, 'data')
    data_dir = os.path.join(cur_dir, 'data')
    os.system("java -Xmx32g -jar \"%s\" \"%s\" \"%s\"" %(os.path.join(root_dir,"XEStoDAFSA","XEStoDAFSA.jar"),data_dir,dataset+".xes"))

    dafsa={}
    path=os.path.join(data_dir,dataset+".xes.txt")
    f=open(path)
    transitions=f.read()
    from_state, event, to_state=0,0,0
    #transforming transitions into dictionary
    transitions=transitions.split('\n')
    for ix,tran in enumerate(transitions):

        res= tran.split(';')
        if len(res)<3:
            break

        from_state, event, to_state = res[0], res[1], res[2]

        if from_state not in dafsa.keys():
            dafsa[from_state]={event:to_state}
        else:
            dafsa[from_state][event] = to_state


    return dafsa

def annotate_state_per_case(data,dafsa_states):
    # dafsa_states = get_DAFSA_Dictionary()

    prev_state = '0'
    curr_state = '0'

    for idx, row in data.iterrows():
        # print(idx)
        curr_state= dafsa_states[prev_state][row['concept:name']]
        data.loc[idx,"prev_state"]= prev_state
        data.loc[idx,"state"]=curr_state

        #for next iteration
        prev_state=curr_state


    return data


def annotate_eventlog_with_state_vectorized(data,dataset,trace_variants):
    dafsa_states=get_DAFSA_Dictionary(dataset)
    # data['prev_state']=pd.Series()
    # data['state']=pd.Series()
    # Annotate per trace variant
    activities=[]
    trace_idx=[]
    activity_index=[]


    for idx,trace in enumerate(trace_variants):
        act= trace.split(',')
        activities+=act
        trace_idx+=[idx]*len(act)
        activity_index+=list(range(0, len(act)))

    trace_variants_df= pd.DataFrame({'trace_variant':trace_idx, 'concept:name':activities, 'activity_index':activity_index})

    #you can enahce the following.
    # First take the unique trace_variants for the combination trace_variant','concept:name','activity_index.
    # Then, swifter.apply.
    trace_variants_df = trace_variants_df.groupby(['trace_variant']).apply(annotate_state_per_case, dafsa_states=dafsa_states).reset_index()
    trace_variants_df.drop(columns=['index'],axis=1, inplace=True)


    # event location per trace
    #use cumsum

    activity_index_log= data.groupby('case:concept:name').cumcount()
    data['activity_index']=activity_index_log


    # Join annotation with event log

    data=data.merge(trace_variants_df, how='inner', on=['trace_variant','concept:name','activity_index'])

    return data

