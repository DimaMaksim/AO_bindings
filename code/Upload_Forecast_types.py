
import numpy as np
import pandas as pd
import time

def updateSQL(filename):
    import urllib
    from sqlalchemy import create_engine
    import sqlalchemy
    #uid='j-gb-client';pwd='Mb1ygZywkGxi8ZQLbVeb'
    PARAMS = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};'
                                     'SERVER=KVCEN14-SQLS004\LITE004;'
                                     'DATABASE=AOForecast;'
                                     'Trusted_Connection=yes')
    engineSQL = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(PARAMS))
    df=pd.read_csv(path+'\\concated_plus_60.csv',sep=";")
    #df=df.iloc[:100,:]
    print(len(df))

    with engineSQL.begin() as connection:
        # delete table before insert
        connection.execute('delete from AO.ForecastTypes')

        df.to_sql(name='ForecastTypes',
                con=connection,
                schema='AO',
                if_exists='append',  # what to do if table exists
                index=True,          # insert pandas indices?
                index_label='No',
                dtype={'lagerId':sqlalchemy.types.INTEGER(),'filialId':sqlalchemy.types.INTEGER(),'engineType':sqlalchemy.types.NVARCHAR(length=50)}
                )


def updateSQL_test(filename):
    import urllib
    from sqlalchemy import create_engine
    import sqlalchemy
    #uid='j-gb-client';pwd='Mb1ygZywkGxi8ZQLbVeb'
    PARAMS = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};'
                                     'SERVER=S-KV-CENTER-S27.officekiev.fozzy.lan;'
                                     'DATABASE=InventorySimul;'
                                     'Trusted_Connection=yes')
    engineSQL = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(PARAMS))
    df=pd.read_csv(path+'\\concated_plus_20.csv',sep=";")
    #df=df.iloc[:100,:]
    print("total: ",len(df))

    with engineSQL.begin() as connection:
        # delete table before insert
        connection.execute('delete from dbo.ForecastType')

        df.to_sql(name='ForecastType',
                con=connection,
                schema='dbo',
                if_exists='append',  # what to do if table exists
                index=True,          # insert pandas indices?
                index_label='No',
                #method='multi',
                dtype={'lagerId':sqlalchemy.types.INTEGER(),'filialId':sqlalchemy.types.INTEGER(),'engineType':sqlalchemy.types.NVARCHAR(length=50)}
                )



#%%






if __name__ == '__main__':
    start = time.time()
    path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\loads'
    updateSQL_test("")
    end = time.time()
    print('Script executed in {:.3f} s'.format(end - start))
#%%
