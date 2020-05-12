


import pandas as pd
import numpy as np
import pyodbc
import pickle
pd.options.mode.chained_assignment = None
class calculate():
    def __init__(self,start,end,FilId):
        self.server='S-KV-CENTER-S27'
        self.start=start
        self.end=end
        self.FilId=FilId
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.con_MD=pyodbc.connect(driver=self.driver,server=self.server,database="4t.dev",uid='j-gb-client',pwd='Mb1ygZywkGxi8ZQLbVeb')
        self.con_SalesHub = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='SalesHub.Dev',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_Master = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='MasterData',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_msp = pyodbc.connect(driver=self.driver,server='kvcen15-sqls003\heavy003',database='dwhclientAnalytics',\
                    uid='j-importOrders-controller',pwd='Mb1ygZywkGxi8ZQLbVeb')

    def get_articules(self):
        query=f"SELECT distinct [LagerId],FilialId as FilId\
             FROM [SalesHub.Dev].[DataHub].[v_SalesStores] \
             where date between {self.start} and {self.end} and \
        	 FilialId={self.FilId} order by FilialId, LagerId"
        df = pd.read_sql_query(query,self.con_SalesHub )
        return df
    def get_ssl(self):
        query=f"SELECT Lagerid as lagerid,SSL FROM [InventorySimul].[dbo].[Z_SSL]\
                where Filid={self.FilId}"
        df = pd.read_sql_query(query,self.con_SalesHub )

        return df
    def get_sales(self,articule):
        sql = f"select  QtySales, \
                StoreQtyDefault,\
                PriceOut,\
                ActivityId\
                from [SalesHub.Dev].[DataHub].[v_SalesStores] \
                where LagerId={articule}\
                and FilialId ={self.FilId}\
                and [Date]>={self.start} and [Date]<={self.end}\
                order by [Date]"
        df = pd.read_sql_query(sql,self.con_SalesHub)
        return df
    def get_rasf(self):
        query = f"SELECT A.lagerid as lagerid,B.rasf as rasf FROM [4t.Dev].[4t].[rasf] as A\
                join [4t.Dev].[4t].[rasfid] as B on A.rasfid=B.rasfid\
                where A.filid={self.FilId}"
        df = pd.read_sql_query(query, self.con_SalesHub)
        return df
    def get_Godnost(self,articule):
        sql=f"SELECT[UsedByDay] FROM [InventorySimul].[dbo].[GodnostDays] where lagerid={articule}"
        df=pd.read_sql_query(sql,self.con_SalesHub)
        return df

    def getMacroid(self,articule):
        sql=f"select [lagerMacroID]  from [MasterData].[sku].[Lagers] \
                where lagerId = {articule}"
        df = pd.read_sql_query(sql, self.con_Master)
        return df.iloc[0].values.item()
    def ZPandSL(self,articule):
        def get_ZP(articule):
            query = f"SELECT ZP,SSL FROM [InventorySimul].[dbo].[ZPandSL]\
                    where Filid={self.FilId} and Lagerid= {articule}"
            df = pd.read_sql_query(query,self.con_SalesHub)
            return df
        r=get_ZP(articule)
        if len(r)==0:
            return 50
        else:
            r=r.values[0]
            r1,r3=r[0],r[1]
            if (r1==None)and(r3==None):
                #z=norm.ppf(SSL)
                #return (z,SSL*100)
                return 50
            elif (r1!=None)and(r3==None):
                r1=float(r1)
                r3=norm.cdf(r1)*100
            elif (r1==None)and(r3!=None):
                r3=float(r3)
                r1=norm.ppf(r3*0.01)
            return float(r3)

    def get_mspbyFil(self):
        sql=f"exec [etl].[proc_getMSPbyFil] ?,?,?"
        df=pd.read_sql_query(sql,params=(self.FilId,self.start[1:-1],self.end[1:-1]),con=self.con_msp)
        return df


class replaceEngine02():
    def __init__(self,start,end):
        self.server='S-KV-CENTER-S27'
        self.start=start
        self.end=end
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.con_SalesHub = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='SalesHub.Dev',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')

    def get_sales(self,LagerId,FilId):
        sql = f"select  QtySales \
                from [SalesHub.Dev].[DataHub].[v_SalesStores] \
                where LagerId={LagerId}\
                and FilialId ={FilId}\
                and [Date]>={self.start} and [Date]<={self.end}\
                order by [Date]"
        df = pd.read_sql_query(sql,self.con_SalesHub)
        return df

    def findsmallsales(self,soursefile):
        #ddd=r'C:\Users\dm.maksymenko\Python\Flask_edition\5filials\res\Recalcmodel\testing pilot\data'
        lagers_r=pd.read_csv(home+f'\\{soursefile}.csv',sep=';')

        lagers=lagers_r[['LagerId','FilId']][lagers_r.AOEngine=="AOEngine02"].values
        print(len(lagers))
        ar=np.empty((0,2))
        i=0
        for item in lagers:
            a=replaceEngine02("'"+start+"'","'"+ end +"'")
            data=self.get_sales(item[0],item[1]).fillna(0).values
            sal=np.count_nonzero(data)
            i+=1
            print(i)
            if sal<15:
                ar=np.append(ar,[item],axis=0)
        self.con_SalesHub.close()
        df_ar=pd.DataFrame(data=ar,columns=['LagerId','FilId'])
        lagers_r=lagers_r.merge(df_ar,how='left',on=['LagerId','FilId'],indicator=True)
        lagers_r.AOEngine[lagers_r._merge=='both']="AOEngine06"
        lagers_r.drop(columns="_merge")
        lagers_r.to_csv(home+f'\\{soursefile}_modified.csv',sep=';',index=False)









def getSampleforTrain(FilId,start,end):
    a=calculate("'"+start+"'","'"+ end +"'",FilId)
    msp_all=a.get_mspbyFil()
    lagers_all=a.get_articules()
    ssl_all=a.get_ssl()
    rasf_all=a.get_rasf()
    print("Filial: ",FilId,len(lagers_all))
    # исключение элитки:
    elitka=pd.read_csv(home+'\\bindingsEngins\\elitka.csv',sep=";")
    elitka=elitka[elitka.filid==FilId]
    df_all = lagers_all.merge(elitka, left_on=['LagerId'],right_on=['lagerid'],how='left', indicator=True)
    lagers=df_all[df_all._merge=='left_only']['LagerId'].values

    k=0
    res=np.empty((0,12))
    smallsales=np.empty((0,1))
    for lager in lagers:
        k+=1;print(FilId,k)
        #lager=lager.item()
        data0=a.get_sales(lager)
        data0[['QtySales','StoreQtyDefault']]=data0[['QtySales','StoreQtyDefault']].fillna(0)
        data0.StoreQtyDefault[data0.StoreQtyDefault.values<0]=0
        data=data0[data0.ActivityId.isnull()].values
        #print(data)
        sal=data[:,0]

        stock=data[:,1]
        price=data[:,2]
        if len(data)>15:


            SalesQuantReal_mean=sal.mean()
            if SalesQuantReal_mean!=0:
                #small sales:
                if np.count_nonzero(sal)<15:
                    smallsales=np.append(smallsales,[lager])

                SalesDiff=np.absolute(np.diff(sal)).mean()
                VarianceRealSales=sal.std()/SalesQuantReal_mean
                Turnover_real=stock.mean()/SalesQuantReal_mean
                Rasf=rasf_all[rasf_all.lagerid==lager].rasf
                if len(Rasf)==0:
                    Rasf=1
                else:
                    if len(Rasf.values)==1:
                        Rasf=float(Rasf.values.item())
                    else:
                        Rasf=float(Rasf.values[0].item())

                Rsf_Sale=Rasf/SalesQuantReal_mean
                godnost=a.get_Godnost(lager)
                ShelfLife=float(godnost.values.item()) if len( godnost)!=0 else 366
                try:
                    PriceMedian=np.median(price)
                except:
                    continue
                SSL=ssl_all[ssl_all.lagerid==lager].SSL
                if len(SSL)==0:
                    SSL=50
                else:
                    if len(SSL.values)==1:
                        SSL=float(SSL.values.item())
                    else:
                        SSL=float(SSL.values[0].item())



                macroId=a.getMacroid(lager)
                msp=msp_all[msp_all.lagerId==lager].avgMSP
                Msp_mean=msp.values.item() if len(msp)!=0 else 1

                res=np.append(res,[[lager,FilId,SalesQuantReal_mean,Turnover_real,SalesDiff,Rsf_Sale,SSL,PriceMedian,\
                                    Msp_mean,ShelfLife,VarianceRealSales,macroId]],axis=0)
        #else:
         #   smallsales=np.append(smallsales,[lager[0]])
    a.con_MD.close()
    a.con_SalesHub.close()
    a.con_Master.close()
    a.con_msp.close()
    print(res)
    #frame=pd.DataFrame(data=smallsales)
    #frame.to_csv(r"C:\Users\dm.maksymenko\Python\Flask_edition\5filials\res\Recalcmodel\testing pilot\data\temp"+f"\\smallsales_{FilId}.csv",sep=";",index=False)

    return res,smallsales

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
    print(recalc.dtypes)
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

    names=[f for f in glob.glob(home+f"\\temp\\id*.csv")]
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
#FilId=2407
#fresh=[1998,2025,2052,2155,2156,2157]
#filials_1=[2264,2269,2265,2226,2234,2229,2679,2256,2268,2257,
 #       2266,2262,2275,2260,2231,2213,2221,2195,2201,2216]


#filials_2=[2212,2236,2238,2243,2237,2240,2241,2246,2220,2118,\
 #  2116,2090,2007,1992,2027,2732,2020,2019,2029,2008]

#filials_3=[2057,2010,2034,2072,2783,2734,2017,2040,2014,2000\
 #           ,2005,1990,2054,2001,2030,2733,2031,1995,2578,2382]

#filials_4=[2016,2023,2024,2056,2113,2132,2115,2123,2122,2134,
            #2481,2114,2133,2673,2112,2131,2151,2170,2154,2184]


filials_4_1=[2016,2023,2024,2056,2113,2132,2115,2123,2122,2134]
filials_4_2=[2481,2114,2133,2673,2112,2131,2151,2170,2154,2184]


start="2020-03-20"; end="2020-05-10"
args=[(filid,start,end) for filid in filials_4_2]
#PrevFileName="AOEngines_20200506194824"
home=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data'
model=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\code\model_5_filials.pickle'

if __name__ == "__main__":

    getSampleforTrain(2025,start,end)
    #CompAll(args)
    #Compute(2269,start, end)
    #Concat()

    #start="2020-02-08"; end="2020-03-08"

    #A=replaceEngine02("'"+start+"'","'"+ end +"'")
    #A.findsmallsales("part_0")
    print("ok")
