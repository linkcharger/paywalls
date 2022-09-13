#%%
from logging import log
from os.path import exists
from os import makedirs
from glob import glob
from tqdm import tqdm
import calendar

import matplotlib.pyplot as plt
import matplotlib.dates as matdates
import numpy as np
import pandas as pd
import seaborn as sns
sns.set_style("whitegrid")

from newsDiet import paywalls

pd.options.display.max_rows = 50
pd.options.display.max_columns = 50







#%% ##################################################################################################################
###################################### aggregating visitor records into counts #######################################
######################################################################################################################
for file in tqdm(glob('outputs/visitors/*.csv')):

    name = file[file.find('visitors')+9:-4]
    if name not in ['nytimes.com-1', 'nytimes.com-2']: continue
    paywallDate = pd.Timestamp(paywalls[name])

    
    df = pd.read_csv(file,  index_col=0).T
    df = df.replace(0, np.nan)
    df.index = pd.to_datetime(df.index)

    countDF = pd.DataFrame(index=df.index, columns=['visitors', 'articlesReadSum', 'articlesReadMean'], )
    countDF.index = pd.to_datetime(countDF.index)

    for date in df.index:
        countDF.loc[date, 'visitors'] = df.loc[date].notna().sum()
        countDF.loc[date, 'articlesReadSum'] = int(df.loc[date].sum())
    countDF['articlesReadMean'] = countDF['articlesReadSum']/countDF['visitors']

    countDF.to_csv(f'outputs/counts/{name}.csv')






#%% ##################################################################################################################
################################################## distributions #####################################################
######################################################################################################################
for file in tqdm(glob('outputs/visitors/*.csv')):
    name = file[file.find('visitors')+9:-4]
    if name not in ['nytimes.com-1', 'nytimes.com-2']: continue
    paywallDate = pd.Timestamp(paywalls[name])
    

    df = pd.read_csv(file,  index_col=0).T
    df.index = pd.to_datetime(df.index)
    
    months = df.index.month.unique()

    fig, axes = plt.subplots(
        ncols=1, nrows=len(months), 
        figsize=(5, 2*len(months)), 
        sharex=True, 
        sharey=True, 
        dpi=300
        )

    fig.suptitle(f'{name}: {paywallDate.strftime("%B %d")}', fontsize=16,)



    for i, month in enumerate(months):
        oneMonth = df.loc[df.index.month == month]

        oneMonth = oneMonth.replace(0, np.nan)
        oneMonth = oneMonth.dropna(axis='columns', how='all')
        oneMonth = oneMonth.replace(np.nan, 0)


        oneMonth = oneMonth.sum(axis='index')    


        try:
            sns.histplot(
                oneMonth, 
                stat='count',
                ax=axes[i], 
                binwidth=1,
                )
            axes[i].set_xlim(0, 30)
            axes[i].set_xlabel('Number of articles read, per week, per individual')
            axes[i].tick_params(labelbottom=True)
            axes[i].set_yscale('log')
            axes[i].set_title(calendar.month_name[month])
        except ValueError:
            continue


    plt.tight_layout()    

    plt.savefig(f'results/{name}/article-distributions.png')
    plt.close('all')













#%% ##################################################################################################################
########################################### visitor and article numbers ##############################################
######################################################################################################################
for file in tqdm(glob('outputs/counts/*.csv')):
    '''
    currently using 'counts.csv' files from manual querying
    could also get the same count files by aggregating the 'visitors.csv' files by day
    '''
    name = file[file.find('counts')+7:-4]
    paywallDate = pd.Timestamp(paywalls[name])


    df = pd.read_csv(file,  index_col=0)
    df.index = pd.to_datetime(df.index)
    ################## aggregate all #################
    # df = df.resample('W').agg({
    #     'visitors':             'sum', 
    #     'articlesReadSum':      'sum', 
    #     'articlesReadMean':     'mean',
    # })             # MEANS you have to aggregate as MEANS!!
    ############### aggregate only sums, then divide ######## -> maybe more robust for bigger periods?
    df = df[['visitors', 'articlesReadSum']]
    df = df.resample('W').sum()
    df['articlesReadMean'] = df['articlesReadSum'] / df['visitors'] 


    df = df.iloc[1:-1]
    ############################################################

    fig, axes = plt.subplots(
        nrows=3, ncols=1, 
        sharex=True, 
        figsize=(7, 12), 
        dpi=300
        )

    fig.suptitle(
        f'{name}\nby week',
        fontsize=20, 
        )



    sns.lineplot(data=df['visitors'], ax=axes[0])
    axes[0].axvline(paywallDate, linewidth=1, color='k')
    axes[0].set_ylabel('Total visitors')

    
    sns.lineplot(data=df['articlesReadSum'], ax=axes[1])
    axes[1].axvline(paywallDate, linewidth=1, color='k')
    axes[1].set_ylabel('Total articles read')


    sns.lineplot(data=df['articlesReadMean'], ax=axes[2])
    axes[2].set_ylabel('Articles per visitor')
    axes[2].axvline(paywallDate, linewidth=1, color='k')
    axes[2].text(
        s=f'paywall: {str(paywallDate)[:10]}', 
        x=0.5, 
        y=0.85,
        transform=axes[1].transAxes,
        horizontalalignment='center',
        )
    
    axes[2].xaxis.set_major_locator(matdates.MonthLocator())
    # axes[1].xaxis.set_major_locator(matdates.WeekdayLocator(interval=4)) # every 4 weeks
    
    plt.xticks(rotation=45, horizontalalignment='right')
    # plt.tight_layout()

    subfolder = f'results/{name}'
    if not exists(subfolder): makedirs(subfolder)
    plt.savefig(f'{subfolder}/readership-evolution.png')
    plt.close('all')














#%% ##################################################################################################################
###################################### visualising diets before and after ############################################
######################################################################################################################
for file in tqdm(glob('outputs/diets/*.csv')):
    name = file[file.find('diets')+6:-4]

    df = pd.read_csv(file, index_col=0)

    pres = [col for col in df.columns if '_pre' in col]
    posts = [col for col in df.columns if '_post' in col]
    sites = [col[:-4] for col in df.columns if '_pre' in col]

    if len(pres) == 0: continue


    pre = df[pres].mean()
    post = df[posts].mean()

    ############################################################################
    ################################# bar charts ###############################
    ############################################################################

    ############################### mean ###################################
    pre = pre.sort_index(ascending=False)
    post = post.sort_index(ascending=False)

    fig, (ax1, ax2) = plt.subplots(
        nrows=1, ncols=2, 
        sharex=True, 
        figsize=(10, 0.2 * len(pre)), 
        dpi=300
        )
    fig.suptitle(
        f'News diets of {name} readers\n(average number of articles read each month)', 
        x=0.60, 
        fontsize=24, 
        )

    ax1.barh(pre.index, pre.values)
    ax1.set_title(
        f'BEFORE the paywall', 
        fontdict={'size': 11, 'weight': 'bold'}
        )
    ax2.barh(post.index, post.values)
    ax2.set_title(
        f'AFTER the paywall', 
        fontdict={'size': 11, 'weight': 'bold'}
        )
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])

    subfolder = f'results/{name}'
    if not exists(subfolder): makedirs(subfolder)
    plt.savefig(f'{subfolder}/diets_before-after_mean.png')
    plt.close('all')


    ############################### pie ###################################
    try:
        pre = pre.sort_values(ascending=False)
        post = post.sort_values(ascending=False)    

        fig, (ax1, ax2) = plt.subplots(
            nrows=1, ncols=2, 
            sharex=True, 
            figsize=(17, 8), 
            dpi=300,
            )

        fig.suptitle(
            f'News diets of {name} readers\n(average number of articles read each month)', 
            # x=0.60, 
            fontsize=20, 
            )

        ax1.pie(
            pre, 
            labels=pre.index,
            normalize=True, 
            autopct='%0.1f%%'
            )
        ax1.set_title('BEFORE paywall', fontdict={'weight': 'bold'})
        ax2.pie(
            post, 
            labels=post.index,
            normalize=True, 
            autopct='%0.1f%%'
            )
        ax2.set_title('AFTER paywall', fontdict={'weight': 'bold'})
        
        plt.tight_layout()

        plt.savefig(f'results/{name}/diets_pie.png')
        plt.close('all')
    except Exception:
        continue
    


    ############################### sum ###################################
    pre = df[pres].sum()
    pre = pre.sort_index(ascending=False)

    post = df[posts].sum()
    post = post.sort_index(ascending=False)


    fig, (ax1, ax2) = plt.subplots(
        nrows=1, ncols=2, 
        sharex=True, 
        figsize=(10, 0.2 * len(pre)), 
        dpi=300
        )
    fig.suptitle(
        f'News diets of {name} readers\n(total number of articles read each month)', 
        x=0.60, 
        fontsize=24, 
        )

    ax1.barh(pre.index, pre.values)
    ax1.set_title(
        f'BEFORE the paywall', 
        fontdict={'size': 11, 'weight': 'bold'}
        )
    ax2.barh(post.index, post.values)
    ax2.set_title(
        f'AFTER the paywall', 
        fontdict={'size': 11, 'weight': 'bold'}
        )
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])

    plt.savefig(f'results/{name}/diets_before-after_sum.png')
    plt.close('all')











#%% ##################################################################################################################
########################################## visualising average changes ###############################################
######################################################################################################################
for file in tqdm(glob('outputs/diets/*.csv')):

    name = file[file.find('diets')+6:-4]
    # print('\n' + f' {name} '.center(80, '#'))

    df = pd.read_csv(file, index_col=0)

    pres = [col for col in df.columns if '_pre' in col]
    posts = [col for col in df.columns if '_post' in col]
    sites = [col[:-4] for col in df.columns if '_pre' in col]

    if len(pres) == 0: continue

    # print(f'the pre and post lists match up: {all(pres[i][:-4] == posts[i][:-5] for i in range(len(pres)))}')

    changes = pd.DataFrame(columns=sites, index=list(df.index))
    for individual in df.index:
        for site in sites:
            changes.loc[individual, site] = df.loc[individual, f'{site}_post'] - df.loc[individual, f'{site}_pre']

    meanChanges = changes.mean().sort_index(ascending=False)


    fig, (ax1, ax2) = plt.subplots(
        nrows=1, ncols=2, 
        sharex=False, 
        figsize=(10, 0.2 * len(meanChanges)), 
        dpi=300
        )

    plt.suptitle(
        f'{name}\nchange of monthly article readership, \nfollowing paywall introduction', 
        x=0.6,
        fontsize=22, 
        )

    ##################################### absolute ##############################################

    ax1.barh(meanChanges.index, meanChanges.values)
    ax1.set_title(
        'absolute',
        fontdict={'size': 9}
        )



    ##################################### relative ################################################
    for site in meanChanges.index:
        meanChanges[site] = meanChanges[site]/df.mean()[f'{site}_pre']
    
    ax2.barh(meanChanges.index, meanChanges.values)
    ax2.set_title(
       'relative',
       fontdict={'size': 9}
        )

    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    plt.savefig(f'results/{name}/average-readership-changes.png')
    plt.close('all')
    









#%% ##################################################################################################################
######################################## a balanced diet? (concentration) ############################################
######################################################################################################################
def gini(lst) -> float:
    s = sorted(lst)
    n = len(lst)
    if n == 0 or sum(s) == 0: return np.nan

    B = sum([numberOfArticlesRead * (n-i) for i, numberOfArticlesRead in enumerate(s)]) / (n * sum(s))
    gini = (1 + (1/n) - 2*B) 

    return round(gini,2)



for file in tqdm(glob('outputs/diets/*.csv')):

    name = file[file.find('diets')+6:-4]

    df = pd.read_csv(file, index_col=0)
    if len(df) == 0: continue

    pres = [col for col in df.columns if '_pre' in col]
    posts = [col for col in df.columns if '_post' in col]


    ginis = pd.DataFrame(index=df.index, columns=['gini_pre', 'gini_post'])

    for ID in ginis.index:
        ginis.loc[ID, 'gini_pre'] = gini(df.loc[ID, pres])
        ginis.loc[ID, 'gini_post'] = gini(df.loc[ID, posts])

    ginis['gini_change'] = ginis['gini_post'] - ginis['gini_pre']


    ############# graphing #############
    try:
        fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, figsize=(8, 12), dpi=300)

        sns.histplot(
            ginis[['gini_pre', 'gini_post']], 
            ax=ax1,
            kde=True, 
            )
        ax1.set_title(f'News diet concentration of \n{name} readers')
        
    

        sns.histplot(
            ginis['gini_change'], 
            ax=ax2,
            kde=True, 
            )
        ax2.set_title(f'Change in news diet concentration of \n{name} readers')
        ax2.axvline(0, linewidth=2, color='k')

        plt.savefig(f'results/{name}/diet-concentration-balance.png')
        plt.close('all')
    except Exception:
        continue


