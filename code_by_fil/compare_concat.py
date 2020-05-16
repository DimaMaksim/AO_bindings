


import numpy as np
import pandas as pd
import glob
from datetime import datetime
import os
from pathlib import Path


def concat(path):
    names=[f for f in glob.glob(path_concat+f"\\*.csv")]
    l_ok=[]
    for i,name in enumerate(names):
            #fil=Path(name).stem[-4:]
            df=pd.read_csv(name,sep=";")
            #l.append(fil)
            l_ok.append(df)
            exec('del df')
    df_ok=pd.concat(l_ok)
    del l_ok
    df_ok=df_ok.drop_duplicates()
    df_ok.rename(columns={'LagerId':'lagerId','FilId':'filialId','AOEngine':'engineType'},inplace=True)
    return df_ok


def main(filename):
    df_con=concat(path_concat)
    df_prev=pd.read_csv(path_prev_bindings,sep=';')
    df=df_prev.merge(df_con,how='outer',on=['lagerId','filialId'],indicator=True)
    mask=(df._merge=='left_only')
    df.loc[mask,'engineType_y']=df.loc[mask,'engineType_x']
    print('NaNs in updated engines: ',df.engineType_y.isnull().sum())
    print('New bindings: ',(df._merge=='right_only').sum())
    updated=(df._merge=='both') & (df.engineType_x!=df.engineType_y)
    print('Updated bindings: ',len(df[updated]))
    df.to_csv(filename,index=False,sep=';')

if __name__=='__main__':
    path_concat=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\temp'
    
    path_prev_bindings=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\loads\concated_95.csv'
    filename=os.path.dirname(path_prev_bindings)+'\\bindings_'+datetime.now().strftime("%Y_%m_%d_%H%M%S")+'.csv'
    main(filename)


#%% выбор элитки

filials=[1991,2199,2022,2069,2407,\
   1998,2025,2052,2155,2156,2157,\
   2264,2269,2265,2226,2234,2229,2679,2256,2268,2257,\
   2266,2262,2275,2260,2231,2213,2221,2195,2201,2216,\
   2212,2236,2238,2243,2237,2240,2241,2246,2220,2118,\
   2116,2090,2007,1992,2027,2732,2020,2019,2029,2008,\
   2057,2010,2034,2072,2783,2734,2017,2040,2014,2000,\
   2005,1990,2054,2001,2030,2733,2031,1995,2578,2382,\
   2016,2023,2024,2056,2113,2132,2115,2123,2122,2134,\
   2481,2114,2133,2673,2112,2131,2151,2170,2154,2184,\
   2290,2161,2145,2186,2153,2648,2746,2176,2183,2125,2129,2105,2126,2077,2187]

