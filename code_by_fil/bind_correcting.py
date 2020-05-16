


import pandas as pd
import numpy as np
import pyodbc
import pickle
from numba import jit


pd.options.mode.chained_assignment = None
class calculate():
    def __init__(self,start,end,FilId):
        self.server='S-KV-CENTER-S27'
        self.start=start
        self.end=end
        self.FilId=FilId
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.con_MD=pyodbc.connect(driver=self.driver,server=self.server,database="4t.dev",uid='j-gb-client',pwd='Mb1ygZywkGxi8ZQLbVeb')
        #self.con_SalesHub = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='SalesHub.Dev',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_SalesHub = pyodbc.connect(DRIVER=self.driver,server=self.server,Trusted_Connection='yes')
        self.con_Master = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='MasterData',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_msp = pyodbc.connect(driver=self.driver,server='kvcen15-sqls003\heavy003',database='dwhclientAnalytics',\
                    uid='j-importOrders-controller',pwd='Mb1ygZywkGxi8ZQLbVeb')


    def get_ssl(self):
        query=f"SELECT Lagerid as lagerid,SSL FROM [InventorySimul].[dbo].[Z_SSL]\
                where Filid={self.FilId}"
        df = pd.read_sql_query(query,self.con_SalesHub )

        return df
    def get_sales(self):
        sql = f"select lagerid,QtySales, \
                StoreQtyDefault,\
                PriceOut,\
                ActivityId\
                from [SalesHub.Dev].[DataHub].[SalesStores] s with (nolock, INDEX(pk_SalesStores))\
                where [Date]>={self.start} and [Date]<={self.end}\
                and FilialId ={self.FilId}"
        df = pd.read_sql_query(sql,self.con_SalesHub)
        return df
    def get_rasf(self):
        query = f"SELECT A.lagerid as lagerid,B.rasf as rasf FROM [4t.Dev].[4t].[rasf] as A\
                join [4t.Dev].[4t].[rasfid] as B on A.rasfid=B.rasfid\
                where A.filid={self.FilId}"
        df = pd.read_sql_query(query, self.con_SalesHub)
        return df


    def get_mspbyFil(self):
        sql=f"exec [etl].[proc_getMSPbyFil] ?,?,?"
        df=pd.read_sql_query(sql,params=(self.FilId,self.start[1:-1],self.end[1:-1]),con=self.con_msp)
        df.rename(columns={'lagerId':'lagerid','avgMSP':'msp'},inplace=True)
        return df

    def get_macro_godn(self):
        sql=f"SELECT a.[lagerId] as lagerid, a.[lagerMacroID] as macroid ,b.UsedByDay as godnost FROM [MasterData].[sku].[Lagers] as a\
	           inner join [InventorySimul].[dbo].[GodnostDays] as b on a.lagerId=b.LagerId "
        df = pd.read_sql_query(sql, self.con_Master)
        return df





import sys
from time import time
t0=time()
def getSampleforTrain(FilId,start,end):
    t1=time()
    print(t1-t0)
    a=calculate("'"+start+"'","'"+ end +"'",FilId)
    print("init OK")
    sales_all=a.get_sales()
    print("sales_all OK")
    sales_all[['QtySales','StoreQtyDefault']]=sales_all[['QtySales','StoreQtyDefault']].fillna(0)
    sales_all.StoreQtyDefault[sales_all.StoreQtyDefault.values<0]=0
    sales_all=sales_all[sales_all.ActivityId.isnull()]
    lagers=sales_all.lagerid.unique()
    lagers=pd.DataFrame(data=lagers,columns=['lagerid'])
    msp_all=a.get_mspbyFil()
    ssl_all=a.get_ssl()
    rasf_all=a.get_rasf()

    macro_all=a.get_macro_godn()
    lagers_info=lagers.merge(msp_all,how='left',on='lagerid')
    lagers_info=lagers_info.merge(ssl_all,how='left',on='lagerid')
    lagers_info=lagers_info.merge(rasf_all,how='left',on='lagerid')
    lagers_info=lagers_info.merge(macro_all,how='left',on='lagerid')
    lagers_info.rasf.fillna(1,inplace=True)
    lagers_info.godnost.fillna(0,inplace=True)
    lagers_info.godnost[lagers_info.godnost==0]=366
    lagers_info.SSL.fillna(50,inplace=True)
    lagers_info.msp.fillna(1,inplace=True)


    #print("Filial: ",FilId,len(lagers_all))
    # исключение элитки:
    elitka=pd.read_csv(home+'\\bindingsEngins\\elitka.csv',sep=";")
    elitka.rename(columns={'LageId':'lagerid'},inplace=True)
    elitka=elitka[elitka.filid==FilId]

    df_all = lagers.merge(elitka, on=['lagerid'],how='left', indicator=True)
    lagers=df_all[df_all._merge=='left_only']['lagerid'].values


    k=0
    res=np.empty((0,12))
    smallsales=np.empty((0,1))
    t2=time()
    print(t2-t1)
    for lager in np.nditer(lagers):
        k+=1;print(FilId,k)
        #lager=lager.item()
        data=sales_all[sales_all.lagerid==lager].values
        sal,stock,price=data[:,1],data[:,2],data[:,3]
        if len(data)>15:
            SalesQuantReal_mean=sal.mean()
            if SalesQuantReal_mean!=0:
                #small sales:
                if np.count_nonzero(sal)<15:
                    smallsales=np.append(smallsales,[lager])
                SalesDiff=np.absolute(np.diff(sal)).mean()
                VarianceRealSales=sal.std()/SalesQuantReal_mean
                Turnover_real=stock.mean()/SalesQuantReal_mean
                inf=lagers_info[lagers_info.lagerid==lager].values[0,:]
                Msp_mean,SSL,Rasf,macroId,ShelfLife=inf[1],inf[2],inf[3],inf[5],inf[5]
                Rsf_Sale=Rasf/SalesQuantReal_mean
                PriceMedian=np.median(price)
                arr=np.array([[lager,FilId,SalesQuantReal_mean,Turnover_real,SalesDiff,Rsf_Sale,SSL,PriceMedian,\
                                             Msp_mean,ShelfLife,VarianceRealSales,macroId]],dtype=float)
                res=np.append(res,arr,axis=0)


    a.con_MD.close()
    a.con_SalesHub.close()
    a.con_Master.close()
    a.con_msp.close()
    return res,smallsales

#%%

def ModelLearn(test):
    repl_dict={"0":"AOEngine07","1":"AOEngine06","2":"AOEngine02"}
    #import xgboost as xgb
    #from sklearn.model_selection import cross_val_score,StratifiedKFold
    #xgc=xgb.XGBClassifier(n_estimators=200,max_depth=5,random_state=400,n_jobs=5)
    #di=r'C:\Users\dm.maksymenko\Python\Flask_edition\5filials\res\Recalcmodel'
    #with open(di+r"\\dataset_for_train_with_elit.pickle","rb") as f:
     #   dataset=pickle.load(f)
    #dataset=dataset[dataset.clust_el!="elit"]
    #x=dataset.drop(columns=["clust","clust_el","Rasf",'Rsf_T',"SafetyStockCost_mean","T","Rsf_Sale_L_T",]);y=dataset.clust_el
    #x2=x[["SalesQuantReal_mean","Turnover_real","SalesDiff","Rsf_Sale","SSL",'PriceMedian','Msp_mean',\
     #     "ShelfLife","VarianceRealSales","macroId"]]
    #print("Обучение модели ......")
    #xgc.fit(x2,y)
    #print("Модель упешно обучена")
    cols=['SalesQuantReal_mean', 'Turnover_real', 'SalesDiff', 'Rsf_Sale', 'SSL',\
       'PriceMedian', 'Msp_mean', 'ShelfLife', 'VarianceRealSales', 'macroId']
    with open(model,"br") as f:
        model_5_filials=pickle.load(f)
    recalc=pd.DataFrame(data=test[:,2:],columns=cols)

    clust=model_5_filials.predict(recalc)

    out=pd.DataFrame(data=test[:,[0,1]],columns=["LagerId","FilId"])
    out["AOEngine"]=clust

    out=out.replace({"AOEngine":repl_dict})
    #print(out.AOEngine.value_counts())
    return out


def Compute(FilId,start,end):
    train,smallsales=getSampleforTrain(FilId,start,end)

    print("Данные успешно извлечены. Предсказание модели ...")
    trained=ModelLearn(train)
    df_smallsales=pd.DataFrame(data=smallsales,columns=['LagerId'])

    trained=trained.merge(df_smallsales,how='left',on=['LagerId'],indicator=True)
    trained.AOEngine[(trained._merge=='both')&(trained.AOEngine=='AOEngine02')]="AOEngine06"
    trained.drop(columns="_merge",inplace=True)
    trained.LagerId=trained.LagerId.astype(int)
    trained.FilId=trained.FilId.astype(int)
    print(f"Идет запись филиал {FilId} .....")
    #plc=r'C:\Users\dm.maksymenko\Python\Flask_edition\5filials\res\Recalcmodel\testing pilot\data'
    trained.to_csv(home+f"\\temp\\id_{FilId}.csv",sep=';',index=False)

def Concat():
    #plc=r'C:\Users\dm.maksymenko\Python\Flask_edition\5filials\res\Recalcmodel\testing pilot\data'
    import glob
    import datetime

    names=[f for f in glob.glob(home+f"\\temp\\id_test*.csv")]
    array=[]
    for name in names:
        df=pd.read_csv(name,sep=';')
        array.append(df)
    files=pd.concat(array,sort=False)
    files.rename(columns={'LagerId':'lagerId','FilId':'filialId','AOEngine':'engineType'},inplace=True)

    date_time=str(datetime.datetime.now())
    for ch in "- :":
        date_time=date_time.replace(ch,"")
    files.to_csv(home+f"\\AOEngines_{date_time[:-7]}.csv",sep=";",index=False)
def CompAll(args):
    with Pool(5) as p:
        p.starmap(Compute,args)
    print("Finish")
    #Concat()

#%%
from multiprocessing import Pool

f0=[1991,2199,2022,2069,2407, 1998,2025,2052,2155,2156,2157]
f1=[2264,2269,2265,2226,2234,2229,2679,2256,2268,2257]
f2=[2266,2262,2275,2260,2231,2213,2221,2195,2201,2216]

f3=[2212,2236,2238,2243,2237,2240,2241,2246,2220,2118]
f4=[2116,2090,2007,1992,2027,2732,2020,2019,2029,2008]
f5=[2057,2010,2034,2072,2783,2734,2017,2040,2014,2000]
f6=[2005,1990,2054,2001,2030,2733,2031,1995,2578,2382]
f7=[2016,2023,2024,2056,2113,2132,2115,2123,2122,2134]
f8=[2481,2114,2133,2673,2112,2131,2151,2170,2154,2184]
f9=[2290,2161,2145,2186,2153,2648,2746,2176,2183,2125,2129,2105,2126,2077,2187]
#%%




#%%

start,end="2020-04-01","2020-05-15"
args=[(filid,start,end) for filid in f7+f8+f9]
#PrevFileName="AOEngines_20200506194824"
home=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data'
model=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\code\model_5_filials.pickle'



if __name__ == "__main__":
    #getSampleforTrain(2025,start,end)
    CompAll(args)
    #Compute(2269,start, end)
    #Concat()

    #start="2020-02-08"; end="2020-03-08"

    #A=replaceEngine02("'"+start+"'","'"+ end +"'")
    #A.findsmallsales("part_0")
    print("ok")
