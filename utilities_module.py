import pandas as pd
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.statistics.traces.log.case_statistics import get_variant_statistics
from pm4py.objects.conversion.log.versions.to_dataframe import get_dataframe_from_event_stream


def read_event_log(dir):
    #TODO: enable CSV and XES files.
    log = pd.read_csv(dir)
    log['time:timestamp'] = pd.to_datetime(log['time:timestamp'], utc=True)
    return log

def read_event_log_xes(dir):
    log = xes_import_factory.apply(dir)
    data = get_dataframe_from_event_stream(log)

    # log = conversion_factory.apply(data)

    return data

def get_dfg_time(data):
    """
    Returns the DFG matrix as a dictionary of lists. The key is the pair of acitivities
    and the value is a list of values
    """
    #moving first row to the last one
    temp_row= data.iloc[0]
    data2=data.copy()
    data2.drop(data2.index[0], inplace=True)
    data2=data2.append(temp_row)

    #changing column names
    columns= data2.columns
    columns= [i+"_2" for i in columns]
    data2.columns=columns

    #combining the two dataframes into one
    data = data.reset_index()
    data2=data2.reset_index()
    data=pd.concat([data, data2], axis=1)

    #filter the rows with the same case
    data=data[data['case:concept:name'] == data['case:concept:name_2']]

    #calculating time difference
    data['time:timestamp']=pd.to_datetime(data['time:timestamp'],utc=True)
    data['time:timestamp_2'] = pd.to_datetime(data['time:timestamp_2'],utc=True)

    data['difference'] = (data['time:timestamp_2'] - data['time:timestamp']).astype(
        'timedelta64[m]')   # in  minutes

    #reformating the data to build the dfg
    data=data[['case:concept:name','concept:name', 'concept:name_2','difference']]
    data=data.set_index(['case:concept:name','concept:name', 'concept:name_2'])



    return data


def get_relative_time(data):
    """
    Returns the event log with the relative time difference of every activity
    """

    #moving first row to the last one
    temp_row= data.iloc[0]
    data2=data.copy()

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
    data['start_event']=False

    data['relative_time'] = (data['time:timestamp'] - data['time:timestamp_2']).astype(
        'timedelta64[m]')   # in  minutes


    ''' In case of the first activity, we set the relative time to the number of days since the start of the log
        to make it an integer. We handle that in the file epsilon_estimation_start_timestamp.py      
    '''

    min_timestamp= data['time:timestamp'].min()
    data.loc[0,'relative_time']= (data.loc[0]['time:timestamp'] - min_timestamp).components.days/7

    data.loc[data['case:concept:name'] != data['case:concept:name_2'], 'relative_time'] = \
        (data.loc[data['case:concept:name'] !=data['case:concept:name_2'], 'time:timestamp'] - min_timestamp).dt.days/7

    data.loc[data['case:concept:name'] != data['case:concept:name_2'], 'start_event']=True
    data.loc[0,'start_event']=True

    data.drop(data.tail(1).index, inplace=True)

    data=data[['case:concept:name','concept:name','time:timestamp','relative_time','start_event']]

    return data