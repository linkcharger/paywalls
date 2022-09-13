#%%
import pandas as pd
import numpy as np

from sklearn import datasets
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import f_regression
import sklearn.metrics as metrics
from scipy import stats




def regressionMetrics(y_true, y_pred):

    r2 =        metrics.r2_score(y_true, y_pred)
    MAE =       metrics.mean_absolute_error(y_true, y_pred) 
    MSE =       metrics.mean_squared_error(y_true, y_pred) 
    RMSE =      np.sqrt(MSE)
    # explained_variance =        metrics.explained_variance_score(y_true, y_pred)
    # mean_squared_log_error =    metrics.mean_squared_log_error(y_true, y_pred)
    # median_absolute_error =     metrics.median_absolute_error(y_true, y_pred)

    print('Obs: '.ljust(10),                        f'{len(y_true):>10}')
    print('R2: '.ljust(10),                         f'{r2:>10.4f}')
    print('MAE: '.ljust(10),                        f'{MAE:>10.4f}')
    print('MSE: '.ljust(10),                        f'{MSE:>10.4f}')
    print('RMSE: '.ljust(10),                       f'{RMSE:>10.4f}')


def addStars(value):
    return f'{value:.3f}' + (
        '*  ' if value < 0.1 else 
        '** ' if value < 0.05 else 
        '***' if value < 0.01 else 
        '   ')




#%%
data = datasets.load_diabetes(as_frame=True)
# print(data['DESCR'])


#%%
X_noConst = data['data']
y = data['target']
X_noConst.columns


# %%
ols = LinearRegression()
ols.fit(X_noConst,y)

coefs = np.append(ols.intercept_,ols.coef_)
y_pred = ols.predict(X_noConst)

X = pd.DataFrame(
    {'const': np.ones(len(X_noConst))}
    ).join(pd.DataFrame(X_noConst))


MSE = (sum((y-y_pred)**2))/(len(X)-len(X.columns))
var_b = MSE * (np.linalg.inv(np.dot(X.T, X)).diagonal())
STEs = np.sqrt(var_b)
t_stats = coefs / STEs

p_values = [
    2 * 
    (1 - stats.t.cdf(
        np.abs(t_stat),             # value to test
        len(X) - len(X.iloc[0])     # degrees of freedom
        )
    )
    for t_stat in t_stats]


coefs = np.round(coefs,4)
STEs = np.round(STEs,3)
t_stats = np.round(t_stats,3)
p_values = np.round(p_values,3)

coefs = pd.DataFrame({
    'coef':     coefs, 
    'std err':  STEs,
    't':        t_stats,
    'P > |t|':  p_values,
    }, 
    index=X.columns)
coefs['P > |t|'] = coefs['P > |t|'].apply(addStars)

print(coefs)
regressionMetrics(y_true=y, y_pred=ols.predict(X_noConst))


################ f stat ##################
ols_restricted = LinearRegression()
X_onlyConst = np.ones(len(X)).reshape(-1, 1)
ols_restricted.fit(X_onlyConst, y)
y_pred_restricted = ols_restricted.predict(X_onlyConst)

RSS_unrestricted = np.sum(np.power(y - y_pred, 2))
RSS_restricted = np.sum(np.power(y - y_pred_restricted, 2))

F_stat = (                                      # difference number of features in restricted and unrestricted model
    ( (RSS_restricted - RSS_unrestricted)    /    (len(X.columns) - 1) ) 
    /
    ( RSS_unrestricted /     ( len(X) - len(X.columns) )     )
)

dfNumerator = len(X.columns) - 1
dfDenominator = len(X) - len(X.columns)

p_F_stat = 1 - stats.f(dfNumerator, dfDenominator).cdf(F_stat)

print('F-statistic: '.ljust(10),                     f'{F_stat:>10.4f}')
print('p(F): '.ljust(10),                       f'{p_F_stat}')



# %%
import statsmodels.api as sm
smModel = sm.OLS(y, X).fit()
smModel.summary()

'''
agrees everywhere, except the order of magnitude of F-test
'''






























# %% turning it into dask
from dask_ml.linear_model import LinearRegression as daskLinearRegression
import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, LocalCluster
import webbrowser

cluster = LocalCluster(
        processes=False,
        n_workers=4, 
    )
client = Client(cluster)
webbrowser.open(client.dashboard_link)
print(client)




#%%
X_noConst = da.from_array(data['data'].values)
y = da.from_array(
    data['target'],
    chunks=1000,
    )

X_noConst

#%%
y




# %%
daskOLS = daskLinearRegression()
daskOLS.fit(X_noConst,y)

coefs = np.append(daskOLS.intercept_,daskOLS.coef_)
y_pred = daskOLS.predict(X_noConst)





#%%
# X = pd.DataFrame(
#     {'const': np.ones(len(X_noConst))}
#     ).join(pd.DataFrame(X_noConst))

X = da.append(
    da.from_array(
        np.ones(len(X_noConst))
        .reshape(-1,1)), 
    X_noConst, 
    axis=1, 
    )
X



#%%
MSE = ( sum(  (y - y_pred)**2)  )   /   (X.shape[0] - X.shape[1]   )
beta_variances = MSE * (np.linalg.inv(np.dot(X.T, X)).diagonal())           # FUK! not implemented in dask
STEs = np.sqrt(beta_variances)
t_stats = coefs / STEs

p_values = [
    2 * 
    (1 - stats.t.cdf(
        np.abs(t_stat),             # value to test
        len(X) - len(X.iloc[0])     # degrees of freedom
        )
    )
    for t_stat in t_stats]


coefs = np.round(coefs,4)
STEs = np.round(STEs,3)
t_stats = np.round(t_stats,3)
p_values = np.round(p_values,3)

coefs = pd.DataFrame({
    'coef':     coefs, 
    'std err':  STEs,
    't':        t_stats,
    'P > |t|':  p_values,
    }, 
    index=X.columns)
coefs['P > |t|'] = coefs['P > |t|'].apply(addStars)

print(coefs)




#%%
regressionMetrics(y_true=y, y_pred=ols.predict(X_noConst))


################ f stat ##################
ols_restricted = LinearRegression()
X_onlyConst = np.ones(len(X)).reshape(-1, 1)
ols_restricted.fit(X_onlyConst, y)
y_pred_restricted = ols_restricted.predict(X_onlyConst)

RSS_unrestricted = np.sum(np.power(y - y_pred, 2))
RSS_restricted = np.sum(np.power(y - y_pred_restricted, 2))

F_stat = (                                      # difference number of features in restricted and unrestricted model
    ( (RSS_restricted - RSS_unrestricted)    /    (len(X.columns) - 1) ) 
    /
    ( RSS_unrestricted /     ( len(X) - len(X.columns) )     )
)

dfNumerator = len(X.columns) - 1
dfDenominator = len(X) - len(X.columns)

p_F_stat = 1 - stats.f(dfNumerator, dfDenominator).cdf(F_stat)

print('F-statistic: '.ljust(10),                     f'{F_stat:>10.4f}')
print('p(F): '.ljust(10),                       f'{p_F_stat}')






#%%
import numpy as np
import dask.array as da

x = da.random.normal(10, 0.1, size=(20000, 20000),   # 400 million element array 
                              chunks=(1000, 1000))   # Cut into 1000x1000 sized chunks
y = x.mean(axis=0)[::100]                            # Perform NumPy-style operations


x.nbytes / 1e9  # Gigabytes of the input processed lazily


# %%
%%time
y.compute()