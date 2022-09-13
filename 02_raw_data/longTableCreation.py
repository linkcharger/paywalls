#%%
import webbrowser
import pandas as pd
from pprint import pprint
from tqdm import tqdm
import csv
from os.path import exists
from os import makedirs

import warnings
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)
pd.options.display.max_rows = 50

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
dask.config.set({'distributed.comm.timeouts.connect': '20s'})

from newsDiet import paywalls, newsSitesList

def floor(test, limit):
    return limit if test < limit else test

def ceiling(test, limit):
    return limit if test > limit else test

colsOfInterest = [
    # 'ref_domain_name', 
    'domain_name',
    'event_date',

    'pages_viewed', 
    'duration', 
    # 'event_time', 

    'hoh_most_education', 
    'census_region', 
    'household_size',
    'hoh_oldest_age', 
    'household_income', 
    'children', 
    'racial_background',
    'connection_speed', 
    'country_of_origin', 
    'zip_code', 
    ]

individualCharacteristics = [
    'hoh_most_education', 
    'census_region', 
    'household_size',
    'hoh_oldest_age', 
    'household_income', 
    'children', 
    'racial_background',
    'connection_speed', 
    'country_of_origin', 
    'zip_code', 
    ]


#%%
if __name__ == '__main__':

    cluster = LocalCluster(
        processes=True,
        n_workers=4, 
    )
    client = Client(cluster)
    webbrowser.open(client.dashboard_link)
    print(client)


    
    for newspaperSite, paywallDate in paywalls.items():
        top10 = [
            'latimes.com', 
            'nypost.com', 
            'bostonglobe.com', 
            'chicagotribune.com', 
            'startribune.com', 
            'newsday.com', 
            'washingtonpost.com', 
            'inquirer.com', 
            'tampabay.com', 
            'sfchronicle.com',
            ]
        if newspaperSite not in top10: continue

        year = int(paywallDate[:4])

        ######################################################################################################
        
        print(f'\n' + f' {newspaperSite} '.center(80, '#'))
        
        prePWmonths = pd.Timestamp(paywallDate).dayofyear/30.5
        postPWmonths = 12 - prePWmonths
        if prePWmonths < 1 or postPWmonths < 1: continue
        
        idealRangeStart = pd.Timestamp(paywallDate) - pd.Timedelta('90 days')
        idealRangeEnd = pd.Timestamp(paywallDate) + pd.Timedelta('90 days')
        yearStart = pd.Timestamp(f'{year}-01-01')
        yearEnd = pd.Timestamp(f'{year}-12-31')
        
        rangeStart = floor(idealRangeStart, yearStart)
        rangeEnd = ceiling(idealRangeEnd, yearEnd)
        datesOfInterest = pd.date_range(rangeStart, rangeEnd, freq='W')
        
        print(f'paywall date: {paywallDate}')
        print(list(datesOfInterest.month.unique()))

        ######################################################################################################

        ddf = dd.read_parquet(
            f'../comscore/parquet/{year}', 
            index='machine_id', 
            columns=colsOfInterest,
            engine='fastparquet',   # you HAVE TO use the same engine to read as you did for creating the parquet files!!!  only then will fast index lookups work - however, you need the pyarrow (or python-snappy) package installed for google's amazing 'snappy' compression algo
            )
        ddf = ddf[ddf['event_date'].between(rangeStart, rangeEnd)]
        
        visitors = list(
            ddf.loc[ddf['domain_name'] == newspaperSite.strip('-12')]
            .index
            .unique()
            .compute()
            )
        # visitors = [13512886]


        for visitor in tqdm(visitors, desc='visitors done: '):
            thisVisitorRows = []


            # load into memory
            df = ddf.loc[visitor].compute()
            df['numberVisits'] = 1           # fill all rows with 1 -> when aggregated, becomes counter

            # get all websites they visited
            allSites = df['domain_name'].unique()
            
            # setup one row with default values
            defaultValues = {
                'machine_id':           visitor, 
                'date':                 '', 
                'day_of_month':         '', 
                'week_of_month':        '', 
                'month':                '', 
                'year':                 '', 

                'domain_name':          '', 
                'news_site_dummy':      0, 
                'number_visits':        0, 
                'number_pages_viewed':  0, 
                'time_spent_on_site':   0, 
                }
            # add individual characteristics
            defaultValues.update(df.loc[visitor, individualCharacteristics].iloc[0].to_dict())     

            
            
            # aggregate by day and website, do not keep individual characteristics
            notIndividualCharacteristics = [col for col in df.columns if col not in individualCharacteristics]
            agged = (
                df[notIndividualCharacteristics]
                .groupby([pd.Grouper(key='event_date', freq='W', ), 'domain_name'])
                .sum()
                .reset_index(drop=False)
                )

            
            

            # were in one individual, 
            # for this individual, for each date, for each site; 
            # fill in the information you have, if not, leave default
            for date in datesOfInterest:
                for site in allSites:

                    # identifying stuff
                    thisRow = defaultValues.copy()
                    # date = pd.Timestamp(date)
                    thisRow['date'] = date.strftime('%Y-%m-%d')
                    thisRow['day_of_month'] = date.day
                    thisRow['week_of_month'] = (date.day - 1)//7 + 1
                    thisRow['month'] = date.month
                    thisRow['year'] = date.year
                    
                    thisRow['domain_name'] = site

                    thisRow['news_site_dummy'] = 1 if site in newsSitesList else 0


                    # aggregation information
                    aggedRowOfInterest = agged.loc[(agged['event_date'] == date) & (agged['domain_name'] == site)]
                    
                    if len(aggedRowOfInterest) != 0:
                        thisRow['number_visits'] = aggedRowOfInterest['numberVisits'].values[0]
                        thisRow['number_pages_viewed'] = aggedRowOfInterest['pages_viewed'].values[0]
                        thisRow['time_spent_on_site'] = aggedRowOfInterest['duration'].values[0]
                      

                    
                    thisVisitorRows.append(thisRow)



            # printing to file
            # one per individual
            subfolder = f'outputs/longTable/{newspaperSite}/individuals'
            if not exists(subfolder): makedirs(subfolder)

            with open(f'{subfolder}/{visitor}.csv', 'w', newline='') as file:
                keys = thisVisitorRows[0].keys()
                dict_writer = csv.DictWriter(file, keys)

                dict_writer.writeheader()
                dict_writer.writerows(thisVisitorRows)

            


        ddf = dd.read_csv(
            f'{subfolder}/*.csv', 
            encoding_errors='replace',
            ).set_index('macmachine_id', npartitions='auto', compute=False)
        
        
        ddf.to_csv(f'outputs/longTable/{newspaperSite}/long_table.csv', single_file=True)

        ddf.to_parquet(
            f'outputs/longTable/{newspaperSite}/parquetLongTable/', 
            engine='fastparquet',               
            compression='snappy', 
            write_index=True, 
            append=False, 
            write_metadata_file=True, 
            schema='infer', 
            )
















# #%% output to parquet
# if __name__ == '__main__':

#     # cluster = LocalCluster(
#     #     processes=True,
#     #     n_workers=4, 
#     # )
#     # client = Client(cluster)
#     # webbrowser.open(client.dashboard_link)
#     # print(client)


    
#     for newspaperSite, paywallDate in paywalls.items():
#         if 'nytimes.com' not in newspaperSite: continue
#         if newspaperSite == 'nytimes.com-1': continue

#         ddf = dd.read_csv(
#             f'outputs/longTable/{newspaperSite}/individuals/*.csv', 
#             encoding_errors='replace',
#             assume_missing=True,
#             ).set_index('machine_id', npartitions='auto', compute=False)


#         ddf.to_parquet(
#             f'outputs/longTable/{newspaperSite}/parquetLongTable/', 
#             engine='fastparquet',               
#             compression='snappy', 
#             write_index=True, 
#             append=False, 
#             write_metadata_file=True, 
#             schema='infer', 
#             )





    

# %%
