


import numpy as np
import pandas as pd
import glob
#import re

import os
from pathlib import Path
path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\temp'
names=[f for f in glob.glob(path+f"\\*.csv")]


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
l=[2679,2275,2269,2268,2266,2265,2264,2262,2260,2256,2234,2231,2229,2226,2157,2156,2155,2052,2025,1998]

df_prev=pd.read_csv(r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\loads\concated_plus_20.csv',sep=';')
df_prev_cut=df_prev[~df_prev.filialId.isin(l)]
df_prev_cut.filialId.value_counts()


df_conc=pd.concat([df_prev_cut,df_ok])

df_conc=df_conc.drop_duplicates()
len(set(df_conc.filialId.values.tolist()))
df_conc.to_csv(r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\loads\concated_plus_40.csv',index=False,sep=';')

#%% выбор элитки
import pyodbc

import numpy as np
import pandas as pd

def get_aoengine_name(filid,conn):
    #sql=f"exec [AO].[GetForecastTypes] ?"
    sql=f"""SET NOCOUNT ON declare @tabl table(lagerid int, clasid int,filtype int,engintype char(64), forecasttype int)
        insert @tabl exec [AO].[GetForecastTypes] {filid}
        select * from @tabl """
    df=pd.read_sql_query(sql,con=conn)
    return df


driver='{ODBC Driver 17 for SQL Server}'
server='KVCEN14-SQLS004.officekiev.fozzy.lan\LITE004'
database='AOForecast'
user='j-ao-bot'
pwd='D4B73186-BFD4-424F-868B-14A9C64299F6'
conn=pyodbc.connect(driver=driver,server=server,database=database,uid=user,pwd=pwd)
get_aoengine_name(1991,conn)

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


temp=[]
for fil in filials:
    df=get_aoengine_name(fil,conn)
    df['filid']=fil
    df_loc=df[df.engintype.str.strip()=='AOEngine05']
    temp.append(df_loc[['filid','lagerid']])
    print(fil)

df=pd.concat(temp)
del temp

conn.close()

path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\bindingsEngins'

df.to_csv(path+'\\elitka_new.csv',sep=';',index=False)
df=df.drop_duplicates()



#%%  разделение на части


import numpy as np
import pandas as pd
path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data'

df=pd.read_csv(r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\AOEngines_20200506194824.csv',sep=';')
n=len(df)
k=1+n//5
for i in (0,1,2,3,4):
    a=df.iloc[i*k:(i+1)*k,:]
    a.to_csv(path+f'\\part_{i}.csv',sep=';',index=False)



#%% слияние


import numpy as np
import pandas as pd
import glob


import os
from pathlib import Path
path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\temp\for_concat'
names=[f for f in glob.glob(path+f"\\*.csv")]
l_ok=[]
for i,name in enumerate(names):

        df=pd.read_csv(name,sep=";")
        print(len(df))
        l_ok.append(df)
        exec('del df')
df_ok=pd.concat(l_ok)
del l_ok


df_ok=df_ok.drop_duplicates()
df_ok.to_csv(path+'\\concated_0805.csv',index=False,sep=';')





#%%


import pyodbc

import numpy as np
import pandas as pd

def get_sales(filid,start,end,conn):
    #sql=f"exec [AO].[GetForecastTypes] ?"
    sql=f"""SELECT [FilialId],[Date],[LagerId],[QtySales],[AmountSales]
           ,[StoreQtyDefault],[AmountDefault],[PriceOut],[ActivityId]
           FROM [SalesHub.Dev].[DataHub].[v_SalesStores] 
           where Date between {start} and {end} and FilialId={filid}"""
    df=pd.read_sql_query(sql,con=conn)
    return df


driver='{ODBC Driver 17 for SQL Server}'
server='S-KV-CENTER-S27.officekiev.fozzy.lan'
database='SalesHub.Dev'
user='j-gb-client'
pwd='Mb1ygZywkGxi8ZQLbVeb'
conn=pyodbc.connect(driver=driver,server=server,database=database,uid=user,pwd=pwd)

start,end,filid="'20200315'","'20200508'",1998
get_sales(filid,start,end,conn)
conn.close()
