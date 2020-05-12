


import pandas as pd
import numpy as np
import pyodbc
import pickle
import json
import pika
import yaml

pd.options.mode.chained_assignment = None
with open("config.yaml") as f:
    config=yaml.full_load(f)

credentials = pika.PlainCredentials(username=config['rabbit']['user'], password=config['rabbit']['pwd'])

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['rabbit']['host'],port=5672,virtual_host=config['rabbit']['vhost'],  credentials=credentials))

channel = connection.channel()
#channel.queue_declare(queue=config['rabbit']['in_queue'])
#channel.queue_declare(queue=config['rabbit']['out_queue'])



class calculate():
    def __init__(self,start,end,FilId):
        self.server='s-kv-center-s27.officekiev.fozzy.lan'
        self.start=start
        self.end=end
        self.FilId=FilId
        self.driver='{ODBC Driver 17 for SQL Server}'
        self.con_MD=pyodbc.connect(driver=self.driver,server=self.server,database="4t.dev",uid='j-gb-client',pwd='Mb1ygZywkGxi8ZQLbVeb')
        self.con_SalesHub = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='SalesHub.Dev',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_Master = pyodbc.connect(DRIVER=self.driver,server=self.server,DATABASE='MasterData',UID='j-gb-client',PWD='Mb1ygZywkGxi8ZQLbVeb')
        self.con_msp = pyodbc.connect(driver=self.driver,server='kvcen15-sqls003.officekiev.fozzy.lan\heavy003',database='dwhclientAnalytics',\
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
                from [SalesHub.Dev].[DataHub].[v_SalesStores] \
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

def forecast(FilId,start,end):

    a=calculate("'"+start+"'","'"+ end +"'",FilId)
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

    # исключение элитки:
    elitka=pd.read_csv('elitka.csv',sep=";")
    elitka.rename(columns={'LageId':'lagerid'},inplace=True)
    elitka=elitka[elitka.filid==FilId]

    df_all = lagers.merge(elitka, on=['lagerid'],how='left', indicator=True)
    lagers=df_all[df_all._merge=='left_only']['lagerid'].values

    with open('model_5_filials.pickle',"br") as f:
        model_5_filials=pickle.load(f)
    k=0

    for lager in lagers:
        k+=1;print(FilId,k)
        smallsales=0
        data=sales_all[sales_all.lagerid==lager].values
        sal,stock,price=data[:,1],data[:,2],data[:,3]
        #data0[['QtySales','StoreQtyDefault']]=data0[['QtySales','StoreQtyDefault']].fillna(0)
        #data0.StoreQtyDefault[data0.StoreQtyDefault.values<0]=0
        #data=data0[data0.ActivityId.isnull()].values

        if len(data)>15:
            SalesQuantReal_mean=sal.mean()
            if SalesQuantReal_mean!=0:
                #small sales:
                if np.count_nonzero(sal)<15:
                    smallsales=1

                SalesDiff=np.absolute(np.diff(sal)).mean()
                VarianceRealSales=sal.std()/SalesQuantReal_mean
                Turnover_real=stock.mean()/SalesQuantReal_mean
                inf=lagers_info[lagers_info.lagerid==lager].values[0,:]
                Msp_mean,SSL,Rasf,macroId,ShelfLife=inf[1],inf[2],inf[3],inf[5],inf[5]
                Rsf_Sale=Rasf/SalesQuantReal_mean
                PriceMedian=np.median(price)

                res=np.array([SalesQuantReal_mean,Turnover_real,SalesDiff,Rsf_Sale,SSL,PriceMedian,\
                                    Msp_mean,ShelfLife,VarianceRealSales,macroId],dtype=float)
                repl_dict={"0":"AOEngine07","1":"AOEngine06","2":"AOEngine02"}

                cols=['SalesQuantReal_mean', 'Turnover_real', 'SalesDiff', 'Rsf_Sale', 'SSL',\
                   'PriceMedian', 'Msp_mean', 'ShelfLife', 'VarianceRealSales', 'macroId']
                recalc=pd.DataFrame.from_dict({"s":res},orient='index',columns=cols)

                clust=model_5_filials.predict(recalc).item()
                engine=repl_dict[clust]
                if smallsales==1:
                    if clust=="2":
                        clust="1"
                out=dict(lagerId=lager.item(),filialId=FilId,engineType=repl_dict[clust])
                channel.basic_publish(exchange='', routing_key=config['rabbit']['out_queue'], body=json.dumps(out))
    a.con_MD.close()
    a.con_SalesHub.close()
    a.con_Master.close()
    a.con_msp.close()

start,end,fil="2020-03-20","2020-05-07",2407
forecast(fil,start,end)



def callback(ch, method, properties, body):
    mes=json.loads(body)
    forecast(mes['filid'],mes['start'],mes['end'])
#channel.basic_consume(queue=config['rabbit']['in_queue'], on_message_callback=callback, auto_ack=True)
#channel.start_consuming()
