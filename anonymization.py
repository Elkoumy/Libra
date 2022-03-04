
import pandas as pd
import numpy as np
from utilities_module import get_relative_time
import math
from scipy.stats import laplace


def draw_anonymized_sample(log, prob=0.15,eps=1.0):
    iteration=1
    #todo: Do we need the privacy accountant?
    new_eps=privacy_accountant(eps,iteration)
    sample=draw_sample(log,prob)
    anonymized_sample=anonymize_sample(sample, new_eps)

    return anonymized_sample


def draw_sample(log, prob):
    case_ids = log['case:concept:name'].unique()
    selected_idx = np.random.choice(case_ids.shape[0], int(prob * case_ids.shape[0]))
    selected_cases = case_ids[selected_idx]
    sample = log[log['case:concept:name'].isin(selected_cases)]

    return sample


def anonymize_sample(sample,eps):
    #get relative time
    sample=get_relative_time(sample)

    #add noise to relative time
    sample['noise']= sample.apply(add_noise,eps=eps, axis=1)
    sample['relative_time']=sample['relative_time']+sample.noise

    #return the time to the original time
    sample=relative_time_to_timestamp(sample)
    return sample

def add_noise(data,eps):

    sens_time = 1

    noise = laplace.rvs(loc=0, scale=sens_time / eps, size=1)[0]

    #Scaling noise because of normalization
    # noise = noise * (data['relative_time_max'] - data['relative_time_min']) + data['relative_time_min']

    # data.start_event == True to enable time shifts before and after (-ve and +ve)
    if data.start_event == True and noise+data['relative_time']<0:
        noise=abs(noise)

    return noise


def privacy_accountant(eps,iteration=1):
    new_eps=eps
    return new_eps


def relative_time_to_timestamp(data):
    # estimating the original range of timestamp in days
    original_range = data.groupby('case:concept:name')['time:timestamp'].min()
    original_range = original_range - original_range.min()
    original_range = original_range.max()
    data['noise_timedelta'] = data.apply(noise_unit_converter, axis=1)

    data['case:concept:name'] = data['case:concept:name'].astype('str')

    # cummulative sum per case.
    data['cumm_noise_timedelta'] = data.groupby(['case:concept:name'])['noise_timedelta'].cumsum()
    # convert the float to timedelta
    data['cumm_noise_timedelta'] = pd.to_timedelta(data['cumm_noise_timedelta'], unit='m')

    # estimating the anonymized range of timestamp
    data['cumm_noise_timedelta_abs'] = data['cumm_noise_timedelta'].abs()
    anonymized_range = data.groupby(['case:concept:name'])['cumm_noise_timedelta_abs'].min()
    anonymized_range = anonymized_range.max()
    # compressing the noise to be within the range

    #overflow fix
    original_range=original_range.total_seconds() / (24 * 60 * 60)
    anonymized_range = anonymized_range.total_seconds() / (24 * 60 * 60)
    compression_factor = 0.5 * original_range / (anonymized_range + original_range)

    data['cumm_noise_timedelta'] = data['cumm_noise_timedelta'] * compression_factor

    #copying origianl timestamp for measuring performance
    data['org_timestamp']=data['time:timestamp']

    data['time:timestamp'] = data['time:timestamp'] + data['cumm_noise_timedelta']
    # data.to_csv("data_after_addition.csv", index=False)
    # Fixing the overflow of time
    data.loc[data['time:timestamp'].isnull(), 'time:timestamp'] = pd.Timestamp.max
    return data

def noise_unit_converter(row):
    res=0
    if row.start_event==True: #timestamp noise (in days)
        # convert das to minutes
        res=row.noise*24*60
        # res=0

    else:
        res=row.noise*1.0
    #
        # res = pd.to_timedelta(row.noise ,unit='s')

    return res