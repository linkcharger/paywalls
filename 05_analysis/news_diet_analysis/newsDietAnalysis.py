#%%
import webbrowser
import pandas as pd
from pprint import pprint
from os import listdir
from tqdm import tqdm

import warnings
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)
pd.options.display.max_rows = 50

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
dask.config.set({'distributed.comm.timeouts.connect': '20s'})

from newsDiet import setupLogging, readCsv, chunker, tqdmChunked, getVisitors, getRegularReaders, getDiets
from newsDiet import paywalls, newsSitesList

def floor(test, limit):
    return limit if test < limit else test

def ceiling(test, limit):
    return limit if test > limit else test

setupLogging('newsDietAnalysis')






#%%#######################################################################################
#################################### article reads #######################################
##########################################################################################
if __name__ == '__main__':

    # cluster = LocalCluster(
    #     processes=True,
    #     # processes=False,
    #     n_workers=4, 
    # )
    # client = Client(cluster)
    # webbrowser.open(client.dashboard_link)
    # print(client)


    #%%
    for newspaperSite, paywallDate in paywalls.items():

        year = paywallDate[:4]
        if newspaperSite not in ['nytimes.com-1', 'nytimes.com-2']: continue
        print(f'\n' + f' {newspaperSite} '.center(80, '#'))

        ######################################################################################################

        prePWmonths = pd.Timestamp(paywallDate).dayofyear/30.5
        postPWmonths = 12 - prePWmonths
        if prePWmonths < 1 or postPWmonths < 1: continue
        
        idealRangeStart = pd.Timestamp(paywallDate) - pd.Timedelta('90 days')
        idealRangeEnd = pd.Timestamp(paywallDate) + pd.Timedelta('90 days')
        yearStart = pd.Timestamp(f'{year}-01-01')
        yearEnd = pd.Timestamp(f'{year}-12-31')
        
        rangeStart = floor(idealRangeStart, yearStart)
        rangeEnd = ceiling(idealRangeEnd, yearEnd)
        datesOfInterest = pd.date_range(rangeStart, rangeEnd, freq='D')

        print(f'paywall date: {paywallDate}')
        print(list(datesOfInterest.month.unique()))

        ######################################################################################################
        '''
        - read chunk of individuals
        - for each individual and date, count articlesRead 
        - print to df
        '''

        ddf = dd.read_parquet(
            f'parquet/{year}', 
            index='machine_id', 
            engine='fastparquet',   # you HAVE TO use the same engine to read as you did for creating the parquet files!!!  only then will fast index lookups work
            )

        visitors = list(ddf.loc[
            (ddf['event_date'].between(rangeStart, rangeEnd)) & 
            (ddf['domain_name'] == newspaperSite.strip('-12'))      # for later paywalls
            ].index.unique().compute())

        zeros = {str(date)[:10]: 0 for date in datesOfInterest}
        articleReads = {ID: zeros.copy() for ID in visitors}


        chunksize = 500
        for chunkOfIndividuals in tqdmChunked(visitors, chunksize):
            chunk = ddf.loc[chunkOfIndividuals].compute()
            onSite = chunk.loc[chunk['domain_name'] == newspaperSite.strip('-12')]

            for date in datesOfInterest:
                dayOnSite = onSite.loc[onSite['event_date'] == date]    # also have df[df.datetime_col.between(start_date, end_date)]


                for ID in dayOnSite.index.unique():
                    onePersonOnThisDayOnThisSite = dayOnSite.loc[ID]
                    articlesReadSum = onePersonOnThisDayOnThisSite['pages_viewed'].sum()

                    articleReads[ID][str(date)[:10]] = articlesReadSum
                    
                    
                
        #     break
        # break
        countDF = pd.DataFrame(articleReads).T
        countDF.to_csv(f'outputs/visitors/{newspaperSite}.csv')
        











#%%#######################################################################################
######################################## diets ###########################################
##########################################################################################
if __name__ == '__main__':

    cluster = LocalCluster(
        processes=True,
        # processes=False,
        n_workers=4, 
    )
    client = Client(cluster)
    webbrowser.open(client.dashboard_link)
    print(client)


    alreadyDone = [file[:-4] for file in listdir('outputs/diets')]


    #%%
    for newspaperSite, paywallDate in paywalls.items():

        if newspaperSite in alreadyDone: continue
        # if newspaperSite != 'newsday.com': continue

        print(f'\n\n' + f' {newspaperSite} '.center(80, '#'))
        year = paywallDate[:4]
        prePWmonths = pd.Timestamp(paywallDate).dayofyear/30.5
        postPWmonths = 12 - prePWmonths



        ################################## load ####################################
        ddf = dd.read_parquet(
            f'parquet/{year}', 
            index='machine_id', 
            engine='fastparquet',   # you HAVE TO use the same engine to read as you did for creating the parquet files!!!  only then will fast index lookups work
            )

        

        ############################## before paywall ##############################
        before = ddf.loc[ddf['event_date'] < pd.Timestamp(paywallDate)]
        
        visitors = getVisitors(before, domain=newspaperSite.strip('-12'))
        regularReaders = getRegularReaders(before, domain=newspaperSite.strip('-12'), visitors=visitors)

        # readerDiets = dict.fromkeys(regularReaders, {}) -> fuuuuuuuuuuuuuuuuuuuuuu, all point to the same object in memory
        zeros = {}
        pres = {f'{site}_pre': 0 for site in newsSitesList}
        posts = {f'{site}_post': 0 for site in newsSitesList}
        zeros.update(pres)
        zeros.update(posts)
        readerDiets = {ID: zeros.copy() for ID in regularReaders}
        
        
        readerDiets = getDiets(ddf, readerDiets=readerDiets, paywallDate=paywallDate, monthsInSample=prePWmonths, pre=True)


        ############################## after paywall ###############################
        after = ddf.loc[ddf['event_date'] > pd.Timestamp(paywallDate)]

        readerDiets = getDiets(ddf, readerDiets=readerDiets, paywallDate=paywallDate, monthsInSample=postPWmonths, pre=False)




        ################################## save ####################################
        dietDF = pd.DataFrame(readerDiets).T
        dietDF.to_csv(f'outputs/diets/{newspaperSite}.csv')

        print(f'done with {newspaperSite}')




















%%#######################################################################################
################################## demographics #########################################
#########################################################################################
if __name__ == '__main__':

    cluster = LocalCluster(
        # processes=True,
        processes=False,
        n_workers=4, 
    )
    client = Client(cluster)
    webbrowser.open(client.dashboard_link)
    print(client)


   
    paywallYears = [2009, 2011, 2012, 2013, 2014, 2015, 2016, 2018, 2019, 2020]  

    for year in paywallYears:
        print(f'\n\n' + f' {year} '.center(80, '#'))

        subfolder = f'{comscoreFolder}/{year}'
        ddf = dd.read_parquet(
            subfolder, 
            index='machine_id', 
            engine='fastparquet', 
            # chunksize=
            )

        allIDs = list(ddf.index.unique().compute())

        demographicColumns = [
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
        demographics = {}

        for chunkOfIDs in tqdmChunked(allIDs, chunksize=1000):
            chunk = ddf.loc[chunkOfIDs].compute()

            for ID in chunkOfIDs:
                oneIndividual = chunk.loc[ID]
                if type(oneIndividual) == pd.DataFrame:
                    demographics[ID] = oneIndividual.iloc[0][demographicColumns].to_dict()
                elif type(oneIndividual) == pd.Series:
                    demographics[ID] = oneIndividual[demographicColumns].to_dict()
                
                # break
            # break
        # print(demographics)
        # break

        demographicsDF = pd.DataFrame(demographics).T
        demographicsDF.to_csv(f'outputs/demographics/demographics_{year}.csv')



































#############################################################################################################################
#############################################################################################################################
#############################################################################################################################
#############################################################################################################################
#############################################################################################################################
#############################################################################################################################
#############################################################################################################################







# #%% testing results
# year = 2014
# subfolder = f'{comscoreFolder}/parquet/{year}/'
# ddf = dd.read_parquet(
#         subfolder, 
#         index='machine_id', 
#         engine='fastparquet', 
#         # chunksize=
#         )
# ddf


# #%%
# ids = ddf.index.compute()
# ids

# #%%
# import random

# sample = random.sample(list(ids), 100)
# sample

# #%% chunking
# %%time

# df = ddf.loc[sample].compute()
# for i, individual in enumerate(sample):
#     one = df.loc[individual]
#     oneBefore = one.loc[one['event_date'] > pd.Timestamp('2014-06-01')]
#     print(f'{i} done')


# #%% individual lookups
# %%time
# for i, individual in enumerate(sample):
#     one = ddf.loc[individual].compute()
#     oneBefore = one.loc[one['event_date'] > pd.Timestamp('2014-06-01')]
#     print(f'{i} done')


# #%% date first
# %%time 
# before = ddf.loc[ddf['event_date'] > pd.Timestamp('2014-06-01')]
# sampleDF = before.loc[sample].compute()

# for i, individual in enumerate(sample):
#     one = sampleDF.loc[individual]
#     print(f'{i} done') if i % 10 == 0 else None


# #%%
# %%timeit
# ddf.loc[146621968.0].loc[ddf['event_date'] > pd.Timestamp('2014-06-01')].compute()







# ######################################## counts ###########################################
# if __name__ == '__main__':

#     # cluster = LocalCluster(
#     #     processes=True,
#     #     # processes=False,
#     #     n_workers=4, 
#     # )
#     # client = Client(cluster)
#     # webbrowser.open(client.dashboard_link)
#     # print(client)


#     #%%
#     for newspaperSite, paywallDate in paywalls.items():

#         year = paywallDate[:4]
#         # if year != '2011': continue
#         print(f'\n' + f' {newspaperSite} '.center(80, '#'))

#         prePWmonths = pd.Timestamp(paywallDate).dayofyear/30.5
#         postPWmonths = 12 - prePWmonths
#         if prePWmonths < 1 or postPWmonths < 1: continue

#         idealRangeStart = pd.Timestamp(paywallDate) - pd.Timedelta('90 days')
#         idealRangeEnd = pd.Timestamp(paywallDate) + pd.Timedelta('90 days')
#         yearStart = pd.Timestamp(f'{year}-01-01')
#         yearEnd = pd.Timestamp(f'{year}-12-31')
        
#         rangeStart = floor(idealRangeStart, yearStart)
#         rangeEnd = ceiling(idealRangeEnd, yearEnd)
#         datesOfInterest = pd.date_range(rangeStart, rangeEnd, freq='D')

#         print(f'paywall date: {paywallDate}')
#         print(list(datesOfInterest.month.unique()))

#         ######################################################################################################

#         '''
#         - get visitors *for whole period*
#         - read chunk of individuals
#         - for each date, count (+=)
#             1) visitors
#             2) articlesRead 
#         - print to df
#         '''
#         ddf = dd.read_parquet(
#             f'E:/comscore/parquet/{year}', 
#             index='machine_id', 
#             engine='fastparquet',   # you HAVE TO use the same engine to read as you did for creating the parquet files!!!  only then will fast index lookups work
#             )


#         visitors = list(ddf.loc[
#             (ddf['domain_name'] == newspaperSite.strip('-12')) &
#             (ddf['event_date'].between(rangeStart, rangeEnd))
#             ].index.unique().compute())

#         zeros = {str(date)[:10]: 0 for date in datesOfInterest}
#         articleReads = {f'{site}_{countVersion}': zeros.copy() for site in newsSites for countVersion in ['visitors', 'articlesReadSum', 'articlesReadMean']}



#         chunksize = 500
#         for chunkOfIndividuals in tqdmChunked(visitors, chunksize):
#             chunk = ddf.loc[chunkOfIndividuals].compute()

#             for date in datesOfInterest:
#                 day = chunk.loc[chunk['event_date'] == date]    # also have df[df.datetime_col.between(start_date, end_date)]


#                 for site in newsSites:
#                     visits = day.loc[day['domain_name'] == site]
#                     if len(visits) == 0: continue

#                     numVisitors = visits.index.nunique()
#                     articlesReadSum = visits['pages_viewed'].sum()
#                     articlesReadMean = articlesReadSum/numVisitors

#                     articleReads[f'{site}_visitors'][str(date)[:10]] += numVisitors
#                     articleReads[f'{site}_articlesReadSum'][str(date)[:10]] += articlesReadSum
#                     articleReads[f'{site}_articlesReadMean'][str(date)[:10]] += articlesReadMean * len(chunkOfIndividuals)/len(visitors)
                    
                
#             # break
#         # break
#         countDF = pd.DataFrame(articleReads).T
#         countDF.to_csv(f'outputs/counts/{newspaperSite}.csv')
        
        






#%% ################################ give ruben his subset ###################################
if __name__ == '__main__':

#     # cluster = LocalCluster(
#     #     processes=True,
#     #     # processes=False,
#     #     n_workers=4, 
#     # )
#     # client = Client(cluster)
#     # webbrowser.open(client.dashboard_link)
#     # print(client)


    for newspaperSite, paywallDate in paywalls.items():

        year = paywallDate[:4]
        if newspaperSite != 'nytimes.com': continue
        print(f'\n' + f' {newspaperSite} '.center(80, '#'))


#         prePWmonths = pd.Timestamp(paywallDate).dayofyear/30.5
#         postPWmonths = 12 - prePWmonths
#         if prePWmonths < 1 or postPWmonths < 1: continue
        
#         idealRangeStart = pd.Timestamp(paywallDate) - pd.Timedelta('90 days')
#         idealRangeEnd = pd.Timestamp(paywallDate) + pd.Timedelta('90 days')
#         yearStart = pd.Timestamp(f'{year}-01-01')
#         yearEnd = pd.Timestamp(f'{year}-12-31')
        
#         rangeStart = floor(idealRangeStart, yearStart)
#         rangeEnd = ceiling(idealRangeEnd, yearEnd)
#         datesOfInterest = pd.date_range(rangeStart, rangeEnd, freq='D')

#         print(f'paywall date: {paywallDate}')
#         print(list(datesOfInterest.month.unique()))

#         ######################################################################################################
        
#         ddf = dd.read_parquet(
#             f'parquet/{year}', 
#             index='machine_id', 
#             engine='fastparquet',   # you HAVE TO use the same engine to read as you did for creating the parquet files!!!  only then will fast index lookups work
#             )


#         visitors = list(ddf.loc[
#             (ddf['domain_name'] == newspaperSite.strip('-12')) &
#             (ddf['event_date'].between(rangeStart, rangeEnd))
#             ].index.unique().compute())

#         subset = ddf.loc[visitors]
#         subset.to_csv(f'outputs/visitor-subsets/{newspaperSite}.csv')

        
        
        
        ################################ create dummy ######################
        ddf = dd.read_csv(
            f'outputs/visitor-subsets/{newspaperSite}.csv', 
            ).set_index('machine_id')

ddf


