
import pandas as pd
import numpy as np
from utilities_module import get_relative_time
import math
from scipy.stats import laplace
from trace_variant_noise_injection import anonymize_traces_compacted


def clip_rare_traces(log,eps,delta):
    #estimate the number of unique trace variants

    # list trace variants and their frequencis
    res = log.groupby('case:concept:name')['concept:name'].apply(lambda x: ';'.join(x)).reset_index()
    res['hash'] = res['concept:name'].apply(hash)
    cnts = res.groupby('hash')['case:concept:name'].count()
    k=0
    k = cnts.shape[0]


    #estimate clipping threshold
    c=estimate_clipping_threshold(eps,k,delta)


    #clip trace variant

    vars_to_keep = cnts[cnts >= c].index
    cases_to_keep = res[res['hash'].isin(vars_to_keep)]['case:concept:name']

    log = log[log['case:concept:name'].isin(cases_to_keep)]

    return log


def draw_anonymized_sample(log, prob=0.15,eps=1.0,c=2):
    iteration=1


    # new_eps=privacy_accountant(eps,gamma, iteration)
    sample=draw_sample(log,prob)

    #clipping
    # sample=clip_trace_variants(sample,c)

    if sample.shape[0]==0:
        return sample #empty sample due to clipping

    anonymized_sample=anonymize_sample(sample, eps)

    return anonymized_sample

def clip_trace_variants(log,c=2):

    #list trace variants and their frequencis
    res = log.groupby('case:concept:name')['concept:name'].apply(lambda x: ';'.join(x)).reset_index()
    res['hash'] = res['concept:name'].apply(hash)
    cnts = res.groupby('hash')['case:concept:name'].count()


    vars_to_keep=cnts[cnts>=c].index
    cases_to_keep=res[res['hash'].isin(vars_to_keep)]['case:concept:name']

    log=log[log['case:concept:name'].isin(cases_to_keep)]

    return log

def draw_sample(log, prob):
    case_ids = log['case:concept:name'].unique()
    selected_idx = np.random.choice(case_ids.shape[0], int(prob * case_ids.shape[0]))
    selected_cases = case_ids[selected_idx]
    sample = log[log['case:concept:name'].isin(selected_cases)]
    # print("sampled cases: %s"%(len(selected_cases)))
    return sample


def anonymize_sample(sample,eps):

    #trace variant anonymization
    mode='sampling'
    sample=anonymize_traces_compacted(sample, mode,eps)

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


def privacy_accountant(b,gamma,iteration=1,alpha=2):
    """
    This function is only for poisson subsample, and alpha>=2.
    Zhu et al 19.
    :param eps:
    :param gamma:
    :param iteration:
    :param alpha:
    :return:
    """
    #b= sensitivity / epsilon
    # b=1/eps
    term1=(1-gamma)**(alpha-1) * (alpha * gamma -gamma+1)
    term2=math.comb(alpha,2) * gamma**2 * (1-gamma)**(alpha-2) * math.exp(eps_of_alpha(2,b))
    term3=0

    if alpha >=3:
        for l in range(3,alpha+1):
            term3+= math.comb(alpha,l) * (1-gamma)**(alpha-l) * gamma**l * math.exp((l-1)* eps_of_alpha(l,b))

    eps_poisson= 1/(alpha-1) * math.log( term1 + term2+ 3*term3 )


    eps_composition=iteration*eps_poisson
    return eps_composition

def estimate_clipping_threshold(eps, k, delta):
    c=1/eps * math.log( 2 * k/delta)
    c=round(c)
    return c

def eps_of_alpha(alpha, b):
    """
    Zhu et al 19. This works only for Laplace Mechanism
    :param alpha:
    :param b:
    :return:
    """

    eps=1/(alpha-1) * math.log( alpha/(2*alpha-1) * math.exp((alpha-1)/b) + (alpha-1)/(2*alpha-1) * math.exp(-alpha/b) )

    return eps


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
    # data['time:timestamp']=data['time:timestamp'].astype('datetime64[]')
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