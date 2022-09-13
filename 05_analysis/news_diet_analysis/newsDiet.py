#%%
from os.path import exists
import pandas as pd
from pprint import pprint
from tqdm import tqdm
from time import time
import logging as log
import sys

import dask.dataframe as dd


#%%###############################################################################################

paywallsDF = pd.read_excel('paywalls.xlsx')
paywallsDF = paywallsDF[['paywall_date', 'url']]

paywalls = {}
for index, row in paywallsDF[:18].iterrows():
        paywalls[row['url']] = str(row['paywall_date'].date())

paywalls['nytimes.com-1'] = '2012-04-01'
paywalls['nytimes.com-2'] = '2017-12-01'


pprint(paywalls)




#%%###############################################################################################

with open('newsSitesList.txt', 'r') as f:
    newsSitesList = [line.strip() for line in f.readlines()]

for site in paywalls.keys():
    if site not in newsSitesList:
        newsSitesList.append(site) 
# print(newsSites)


################################################################################################
################################################################################################





def setupLogging(name:str):
    log.basicConfig(
        format='%(asctime)s \t %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S', 
        filename=f'{name}.log', 
        filemode='a', 
        level=log.DEBUG, 
        )

    # def logExceptionToFile(*exc_info):
    #     text = "".join(traceback.format_exception(*exc_info))
    #     log.error("Unhandled exception: %s", text)

    def logExceptionToFile(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        log.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = logExceptionToFile

def getCleanDDFfromCSV(path, dt, scheduler=None):
    
    ############################## ridding of superfluous cols ###############################
    ddf = dd.read_csv(path)
    for col in dt.copy().keys():
        if col not in ddf.columns:
            del dt[col]


    ###################################### converters ########################################
    def coerceToFloat(something):
        try: return float(something)
        except ValueError: return -1.

    def coerceToInt(something):
        try: return int(float(something))           # tries to convert string to int in decimal number system, gets confused by having to parse the comma
        except ValueError: return -1

    def coerceToStr(something):
        try: return str(something)
        except ValueError: return 'nan'

    floatColumns = [column for column, dtype in dt.items() if dtype == 'float']
    intColumns = [column for column, dtype in dt.items() if dtype == 'int']
    strColumns = [column for column, dtype in dt.items() if dtype == 'str']

    converters = {}
    converters.update(dict.fromkeys(floatColumns, coerceToFloat))
    converters.update(dict.fromkeys(intColumns, coerceToInt))
    converters.update(dict.fromkeys(strColumns, coerceToStr))


    ##################################### point to data #########################################
    ddf = dd.read_csv(

        ################# actual data stuff #################
        path, 
        usecols=dt.keys(),
        converters=converters,
        keep_default_na=True,
        

        ################# auxiliary fixes #################
        encoding='utf8',                #'utf8', #'unicode_escape', 'latin-1'
        encoding_errors='replace',      
        on_bad_lines='skip',
        # low_memory=False,             # in pandas, tells it not to read in chunks for consistent dtypes
        # blocksize=None,                 # function may file if csv contains quoted strings within field -> None tells it not to split files into multiple partitions, at cost of reduced parallelism (in fact, it totally ruins any parallelisation, turning it back to pandas basically)
        )


    if 'event_date' in dt.keys():                                           
        ddf['event_date'] = dd.to_datetime(
            ddf['event_date'], 
            errors='coerce', 
            exact=False,                    # allows match to be found anywhere in string
            infer_datetime_format=True,     # if no 'format' is given, infer it -> potential speed-up, we only look at one year at a time, when the format should be consistent
            )
    
    if scheduler:
        return scheduler.persist(ddf)
    else:
        return ddf

def partialReadFile(file, start, end):
    with open(file) as f:
        for i, line in enumerate(f):
            if i in range(start, end):
                print(f'{i:10} {line.strip()}')
            if i > end: 
                break

def chunker(lst, chunksize):
    for i in range(0, len(lst), chunksize):
        yield lst[i:i+chunksize]

def tqdmChunked(lst, chunksize, **kwargs):
    return tqdm(chunker(lst, chunksize=chunksize), total=int(len(lst)/chunksize), unit_scale=chunksize, **kwargs)

def getVisitors(ddf:dd, domain:str):
    print(f' getting all visitors '.center(60, '-'))

    if exists(f'outputs/visitors/{domain}.txt'):
        print('found list of site visitors, reading...')
        with open(f'outputs/visitors/{domain}.txt', 'r') as f:
            individualsThatAccessedTheSite = list(map(float, [line.strip('\n') for line in f.readlines()]))

    else:
        print('didnt find list of site visitors, calculating...')
        domainFilter = ddf['domain_name'].isin([domain])
        individualsThatAccessedTheSite = list(ddf.loc[domainFilter].index.unique().compute())
        with open(f'outputs/visitors/{domain}.txt', 'w') as f:
            f.write('\n'.join(map(str, individualsThatAccessedTheSite)))
    print(f'number of all visitors: {len(individualsThatAccessedTheSite)}')
    return individualsThatAccessedTheSite

def getRegularReaders(ddf:dd, domain:str, visitors:list, chunksize=500):
    
    print(f' getting regular visitors '.center(60, '-'))

    ######################## find the already-vetted ########################
    alreadyReaders = []
    alreadyNotReaders = []
    if exists(f'outputs/readers/{domain}.txt'):
        with open(f'outputs/readers/{domain}.txt', 'r') as f:
            alreadyReaders = list(map(float, [line.strip('\n') for line in f.readlines()]))
    if exists(f'outputs/readers/{domain}Not.txt'):
        with open(f'outputs/readers/{domain}Not.txt', 'r') as f:
            alreadyNotReaders = list(map(float, [line.strip('\n') for line in f.readlines()]))
    
    remainingVisitors = [individual for individual in visitors if individual not in alreadyReaders and individual not in alreadyNotReaders]
    
    print(f'found {len(alreadyReaders)} readers and {len(alreadyNotReaders)} non-readers that were already vetted - {len(remainingVisitors)} visitors left to vet')
    

    ######################## do filtering ########################
    for chunkOfIndividuals in tqdmChunked(remainingVisitors, chunksize=chunksize):
        chunk = ddf.loc[chunkOfIndividuals].compute()

        for individual in chunkOfIndividuals:

            oneReader = chunk.loc[individual]
            if type(oneReader) == pd.DataFrame:
                oneReaderNews = oneReader.loc[oneReader['domain_name'] == domain]
                monthsTheyAccessed = oneReaderNews['event_date'].dt.month.unique()
            elif type(oneReader) == pd.Series:
                if oneReader['domain_name'] == domain:
                    monthsTheyAccessed = [1]

            if len(monthsTheyAccessed) < 3: 
                with open(f'outputs/readers/{domain}Not.txt', 'a') as f: f.write(f'{individual}\n')
            else:
                with open(f'outputs/readers/{domain}.txt', 'a') as f: f.write(f'{individual}\n')

    if exists(f'outputs/readers/{domain}.txt'):
        with open(f'outputs/readers/{domain}.txt', 'r') as f:
            return list(map(float, [line.strip('\n') for line in f.readlines()]))
    else:
        return []

def getDiets(ddf: dd, readerDiets:dict, paywallDate: str, monthsInSample:float, pre:bool):
    global newsSitesList
    readerIDs = list(readerDiets.keys())

    print(f' getting diets of {len(readerIDs)} individuals '.center(60, '-'))
    print('(pre-paywall)' if pre else '(post-paywall)')

    # IDsThatExist = list(ddf.index.unique().compute())
    # IDsThatWeWantAndExist = list(set(IDsThatExist).intersection(readerIDs))
    # IDsThatWeWantAndExist = list(map(int, IDsThatWeWantAndExist))
    # print(f'{len(IDsThatExist)} --> {len(IDsThatWeWantAndExist)}')  
    # inMem = ddf.loc[IDsThatWeWantAndExist].compute()      
    # wtfffff why doesnt this work??!
    # 'do you have this index?' - 'yes i do' - 'okay good, pleas give it to me' - 'no.'
    # ffs



    for individual in tqdm(readerIDs):

        try:
            oneReader = ddf.loc[individual].compute()
        except KeyError:
            continue

        if pre:
            oneReader = oneReader.loc[oneReader['event_date'] < pd.Timestamp(paywallDate)]
        else:
            oneReader = oneReader.loc[oneReader['event_date'] > pd.Timestamp(paywallDate)]


        oneReadersNews = oneReader.loc[oneReader['domain_name'].isin(newsSitesList)]
        visitCounts = oneReadersNews.groupby('domain_name')['pages_viewed'].sum()
        
        for site in newsSitesList:
            colName = f'{site}_pre' if pre else f'{site}_post'

            if site in visitCounts.index:
                readerDiets[individual][colName] = round(visitCounts.loc[site]/monthsInSample, 2)
            

    return readerDiets





################################################################################################
################################################################################################


































#%%
########################################################################################################
##########################################  ARCHIVE  ###################################################
########################################################################################################
########################################################################################################

# def readCsv(subfolder, dt, divs=False):
    
#     ############################## ridding of superfluous cols ###############################
#     ddf = dd.read_csv(f'{subfolder}/*.csv' if divs else subfolder)
#     for col in dt.copy().keys():
#         if col not in ddf.columns:
#             del dt[col]


#     ###################################### converters ########################################
#     def coerceToFloat(something):
#         try:
#             return float(something)
#         except ValueError:
#             return -1.0

#     def coerceToInt(something):
#         try:
#             return str(something)
#         except ValueError:
#             return -1

#     def coerceToStr(something):
#         try:
#             return str(something)
#         except ValueError:
#             return 'nan'

#     floatColumns = [column for column, dtype in dt.items() if dtype == 'float']
#     intColumns = [column for column, dtype in dt.items() if dtype == 'int']
#     strColumns = [column for column, dtype in dt.items() if dtype == 'str']

#     converters = {}
#     converters.update(dict.fromkeys(floatColumns, coerceToFloat))
#     converters.update(dict.fromkeys(intColumns, coerceToInt))
#     converters.update(dict.fromkeys(strColumns, coerceToStr))


#     ##################################### point to data #########################################
#     ddf = dd.read_csv(

#         ################# actual data stuff #################
#         f'{subfolder}/*.csv' if divs else subfolder, 
#         usecols=dt.keys(),
#         dtype=dt,
#         converters=converters,
#         # parse_dates=['event_date'] if 'event_date' in dt.keys() else [],
#         assume_missing=True,
        

#         ################# auxiliary fixes #################
#         # engine='python',
#         # encoding=None,                # legacy: in pandas v1.2, if set to none, passes errors='replace' to open()
#         encoding='utf8',                #'utf8', #'unicode_escape', 'latin-1'
#         encoding_errors='replace',      # doesnt work, need to re-encode the raw csv file
#         low_memory=False,
#         on_bad_lines='skip',
#         )

#     if divs:
#         with open(f'{subfolder}/divisions.txt', 'r') as f:
#             divisions = tuple(map(float, f.read().strip('()').split(', ')))
#         ddf = ddf.set_index('machine_id', sorted=True, divisions=divisions) 

#     ddf[intColumns] = dd.to_i

#     ddf['event_date'] = dd.to_datetime(ddf['event_date'], errors='coerce')
        
#     return ddf
#%% extract all their browsing habits
# with open('outputs/NYTreaders.txt', 'r') as f:
#     IDs = list(map(float, [line.strip('\n') for line in f.readlines()]))


# NYTreadersBrowsing = before.loc[IDs]
# NYTreadersBrowsing.to_csv('outputs/NYTreadersBrowsing.csv', compute=True)







# #%%
# subfolder = f'{comscoreFolder}/indexed/byID/NYT_pre'
# individuals = [66500673.0, 66771622.0, 70320981.0, 71257195.0, 71519475.0, 71784133.0, 73348914.0, 63550364.0, 63897288.0, 64514462.0, 66024610.0, 67309947.0, 69320916.0, 68218043.0, 71698787.0, 71906395.0, 71900652.0, 73120273.0, 73375544.0, 73189803.0, 73778751.0, 73147263.0, 74034904.0, 73996389.0, 74685017.0, 74810710.0, 74834220.0, 74950166.0, 74964828.0, 75318361.0, 75488275.0, 76083482.0, 76213857.0, 76684607.0, 76784426.0, 76788910.0, 76889466.0]
# with open(f'{subfolder}/divisions.txt', 'r') as f: 
#     divisions = tuple(map(float, f.read().strip('()').split(', ')))

# ddf = readCsv(f'{subfolder}/*.csv', dtypesFloat)
# ddf = ddf.set_index('machine_id', sorted=True, divisions=divisions) 

# chunk = ddf.loc[individuals].compute()

# #%%
# readers = {}
# for individual in individuals:
#     readers[individual] = {}

#     oneReader = chunk.loc[individual]
#     oneReadersNews = oneReader.loc[oneReader['domain_name'].isin(newsSites)]
#     visitCounts = oneReadersNews.groupby('domain_name')['pages_viewed'].sum()

#     for site in newsSites:
#         if site in visitCounts.index:
#             readers[individual][site] = int(visitCounts.loc[site])
#         else:
#             readers[individual][site] = 0

# pprint(readers, compact=True) 
                


####################### by domain #######################
# byDomain = ddf.set_index('domain_id', npartitions='auto', compute=False)
# print(byDomain)

# subfolder = f'{comscoreFolder}/indexed/{year}/byDomain'
# if not exists(subfolder): makedirs(subfolder)

# byDomain.to_csv(f'{subfolder}/{year}_byDomain_*.csv', compute=True)

# with open(f'{subfolder}/{year}_byDomain_divisions.txt', 'w') as f:
#     f.write(str(byDomain.divisions))





# def vetIndividual(before: dd, individual:float):
#     oneReader = before.loc[individual]
#     oneNYTreader = oneReader.loc[oneReader['domain_name'].isin([domain])]
#     oneNYTreader = oneNYTreader.compute() 
        
#     monthsTheyAccessed = oneNYTreader['event_date'].dt.month.unique()

#     if len(monthsTheyAccessed) < 3: 
#         with open('outputs/NYT~readers.txt', 'a') as f: f.write(f'{individual}\n')
#     else:
#         with open('outputs/NYTreaders.txt', 'a') as f: f.write(f'{individual}\n')


# for chunkOfIndividuals in chunker(individualsThatAccessedTheSite):
#     start = time()

#     futures = client.map(vetIndividual, before, chunkOfIndividuals, pure=False)
#     client.gather(futures)

#     print(f'{round(time() - start)}s --> {round((time() - start)/50, 1)}s/it')



# chop it up
# def load(before: dd, individual:float):
#     oneReader = before.loc[individual]
#     oneNYTreader = oneReader.loc[oneReader['domain_name'].isin([domain])]
#     return oneNYTreader.compute() 

# def areTheyRegular(oneNYTreader: pd.DataFrame):
#     monthsTheyAccessed = oneNYTreader['event_date'].dt.month.unique()

#     return len(monthsTheyAccessed) == 3

# def save(regular: bool, individual):
#     if regular:
#         with open('outputs/NYTreaders.txt', 'a') as f: f.write(f'{individual}\n')
#     else:
#         with open('outputs/NYT~readers.txt', 'a') as f: f.write(f'{individual}\n')
#     return None


# for chunkOfIndividuals in chunker(individualsThatAccessedTheSite):
#     futures = []
#     start = time()

#     for individual in chunkOfIndividuals:
#         a = client.submit(load, before, individual, retries=5, )
#         b = client.submit(areTheyRegular, a, retries=5)
#         c = client.submit(save, b, individual, pure=False, retries=5)
#         futures.append(c)

#     results = client.gather(futures, errors='skip')
#     del futures, results
#     print(f'{round(time() - start)}s --> {round((time() - start)/50, 1)}s/it')












#%%
# from time import sleep
# def inc(x):
#     sleep(1)
#     return x + 1

# futures = client.map(inc, range(10), pure=True)
# results = client.gather(futures) 
# results





























