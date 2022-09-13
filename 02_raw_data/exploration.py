#%%
from hashlib import new
import dask
import dask.dataframe as dd
import dask.delayed as delayed
import pandas as pd
from numpy import dtype, number
from pprint import pprint

import time
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)


def coerceToFloat(something):
    try:
        return float(something)
    except ValueError:
        return -1

def coerceToInt(something):
    try:
        return int(something)
    except ValueError:
        return -1

def coerceToString(something):
    try:
        return str(something)
    except Exception:
        return ''

def daskReadCSV(filename, dt):
        
    ddf = dd.read_csv(filename)

    # print('number of columns (and dtypes):', len(dt))
    for col in dt.copy().keys():
        if col not in ddf.columns:
            del dt[col]
    # print('number of columns (and dtypes) - dropped superfluous:', len(dt))



    floatColumns = [column for column, dtype in dt.items() if dtype == 'float']
    intColumns = [column for column, dtype in dt.items() if dtype == 'int']
    strColumns = [column for column, dtype in dt.items() if dtype == 'str']


    converters = {}
    converters.update(dict.fromkeys(floatColumns, coerceToFloat))
    converters.update(dict.fromkeys(intColumns, coerceToInt))


    print(f'reading {filename[filename.find("20"):filename.find("20")+4]} with {len(dt)} columns')
    ddf = dd.read_csv(
        # actual data stuff
        filename, 
        usecols=dt.keys(),
        dtype=dt,
        parse_dates=['event_date'] if 'event_date' in dt.keys() else [],
        
        # auxiliary fixes
        encoding='unicode_escape',
        encoding_errors='replace',
        low_memory=False,
        on_bad_lines='skip',
        converters=converters,
        )
        
    return ddf




comscoreFolder = 'E:/csv/'
# comscoreFolder = 'D:/csv/'




#%%
# https://tutorial.dask.org/05_distributed.html
# https://tutorial.dask.org/06_distributed_advanced.html
# https://tutorial.dask.org/08_machine_learning.html#Training-on-Large-Datasets 

# high-performance computations: https://docs.dask.org/en/latest/how-to/deploy-dask-clusters.html 

from dask.distributed import Client
# dask.config.set(scheduler='threaded')
client = Client(processes=False, n_workers=4)
client.cluster







#%%
dt = {
        'machine_id':               'int', 
        'site_session_id':          'int', 
        'user_session_id':          'int', 
        'domain_id':                'int',
        'ref_domain_name':          'str', 
        'ref_domain__name':         'str', 

        'pages_viewed':             'int', 
        'duration':                 'int', 
        'event_date':               'str',
        # 'event_time':               'str', 
    
        # 'hoh_most_education':       'int', 
        # 'census_region':            'int',
        # 'household_size':           'int', 
        # 'hoh_oldest_age':           'int', 
        # 'household_income':         'int', 
        # 'children':                 'int',
        # 'racial_background':        'int', 
        # 'connection_speed':         'int', 
        # 'country_of_origin':        'int',
        
        'zip_code':                 'int', 
        'domain_name':              'str',
    }

# ddf = daskReadCSV(f'{comscoreFolder}y2011*', dt).head(10_000_000, npartitions=30)
ddf = daskReadCSV(f'{comscoreFolder}y2011*', dt)
ddf







#%% #######################################################################################################
######################################### descriptives ####################################################
###########################################################################################################
# only works for ddf.head()
ddf['ref_domain_name'].value_counts(normalize=True, dropna=False).to_frame().to_excel('descriptives/value_counts()-ref_domain_names.xlsx')
ddf['domain_name'].value_counts(normalize=True, dropna=False).to_frame().to_excel('descriptives/value_counts()-domain_names.xlsx')






#%% id investigating
ddf.sort_values('user_session_id', inplace=True)


#%% finding how many nytimes readers in 2008-2015
nyReaderNumbersOverTheYears = {}
nyReadersOverTheYears = {}

for year in [str(y) for y in range(2008, 2016)]:
    
    dt = {
        'machine_id':               'int', 
        'domain_name':              'str',
        }

    ddf = daskReadCSV(f'{comscoreFolder}y{year}*', dt)

    nyReaders = ddf.loc[ddf['domain_name'].isin(['nytimes.com'])]['machine_id'].unique()

    numberReaders, listReaders = dask.compute(
        delayed(len)(nyReaders), 
        delayed(list)(nyReaders)
        )

    nyReaderNumbersOverTheYears[year] = numberReaders
    nyReadersOverTheYears[year] = listReaders

    print(f'{year}: {numberReaders:10}')

nyReaderNumbersOverTheYears




pd.Series(nyReaderNumbersOverTheYears).to_frame().to_excel('descriptives/nytReaderNumbers_OnceAYear.xlsx')
pd.Series(nyReadersOverTheYears).to_frame().T.to_excel('descriptives/nytReaders_OnceAYear.xlsx')






























#%%
''' ########################################################################################################################
################################################## DiD aggregation #########################################################
############################################################################################################################

newsThisYear = list(paywalls['url'])

#%%
for newspaper in newsThisYear[:]:
    # print(ddf[ddf['domain_name'] == newspaper])
    print(any(ddf['domain_name'] == newspaper))




#%%

# find all rows with news accesses
# group by date
# sum visits
# make one new row for this day and county, with list of non-zero-visited news sites
# then explode that list in the 'domain_name' column, duplicating all the values in the other columns of the example row
# merge on date .. and add the newspaper column (with duplicate dates) at the same time?

# surrogate, for now
nyt = pd.read_excel(
    'nytimes2011.xlsx',
    usecols=dt.keys(),
    dtype=dt,
    parse_dates=['event_date'],
    )
nyt


# onlyNewsDF = ddf.loc[ddf['domain_name'].isin(newsThisYear)]
# newsvisits = onlyNewsDF.groupby(['event_date', 'zip_code', 'domain_name'])['pages_viewed'].sum()
newsvisits = nyt.groupby(['event_date', 'zip_code', 'domain_name'])['pages_viewed'].sum()
newsvisits = newsvisits.to_frame().reset_index()

newsvisits



#%%
for day in newsvisits['event_date'].unique()[:3]:
    for zip in newsvisits['zip_code'].unique()[:3]: #even if it doesnt exist for this day

        # find missing newspapers
        condition = (newsvisits['event_date'] == day) & (newsvisits['zip_code'] == zip)
        alreadyExistingNewspapers = [site for site in newsvisits[condition]['domain_name']]

        missingNewspapers = [newspaper for newspaper in newsThisYear if newspaper not in alreadyExistingNewspapers]

        # make row with same date and postcode, zero pages_viewed, list of missing newspapers in 'domain name' for later explosion
        fillerRow = pd.Series({
            'event_date':   day, 
            'zip_code':     zip, 
            'domain_name':  missingNewspapers, 
            'pages_viewed': 0,
        })

        newsvisits = newsvisits.append(fillerRow, ignore_index=True)

newsvisits = newsvisits.explode('domain_name')
newsvisits = newsvisits.set_index(['event_date', 'zip_code', 'domain_name'])

newsvisits






#%%
aggregationDF = pd.DataFrame()
aggregationDF['allVisits'] = ddf.groupby(['event_date', 'zip_code', ])['pages_viewed'].sum()



aggregationDF = aggregationDF.join(newsvisits)


aggregationDF







#%%
aggregationDF.loc[aggregationDF['newsVisits'].isin(range(10))]


#%%
aggregationDF[aggregationDF['newsVisits'].notna()]


#%%



'''














#%%
nyt = pd.read_excel('nytimes2011.xlsx', usecols=dt.keys(), parse_dates=['event_date'])


#%%
import matplotlib
nyt.groupby('event_date')['pages_viewed'].sum().plot(figsize=(10,6))
matplotlib.pyplot.axvline('2011-03-28')
matplotlib.pyplot.savefig('nytimes.png')



# %%
nyt.groupby(['event_date', 'zip_code'])['pages_viewed'].sum()








#%%
# paywalls = pd.read_stata('paywalls/newspaper_paywall_list.dta')
# paywalls = paywalls.dropna(axis='index', subset=['paywall_date'])
# # paywalls = paywalls[['newspaper', 'paywall_date']]

# # dateCondition = (paywalls['paywall_date'] > '2011-01-01') & (paywalls['paywall_date'] < '2011-12-31')
# # paywalls = paywalls[dateCondition]


# paywalls['url'] = ''
# paywalls.to_excel('paywalls.xlsx')


paywalls = pd.read_excel('paywalls.xlsx')
paywalls = paywalls[['newspaper', 'paywall_date', 'url']]

dateCondition = (paywalls['paywall_date'] > '2011-01-01') & (paywalls['paywall_date'] < '2011-12-31')
paywalls = paywalls[dateCondition]

paywalls

