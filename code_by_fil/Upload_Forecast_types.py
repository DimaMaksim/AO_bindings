
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
    df=pd.read_csv(path+'\\concated_plus_80.csv',sep=";")
    #df=df.iloc[:100,:]
    print(len(df))

    with engineSQL.begin() as connection:
        # delete table before insert
        connection.execute('delete from AO.ForecastTypes')
        print("GO
        df.to_sql(name='ForecastTypes',
                con=connection,
                schema='AO',
                if_exists='append',  # what to do if table exists
                index=True,          # insert pandas indices?
                index_label='No',
                method='multi',
                chunksize=100,
                dtype={'lagerId':sqlalchemy.types.INTEGER(),'filialId':sqlalchemy.types.INTEGER(),'engineType':sqlalchemy.types.NVARCHAR(length=50)}
                )


if __name__ == '__main__':
    start = time.time()
    path=r'c:\Users\dm.maksymenko\Python\AOEngine_bind\data\loads'
    updateSQL("")
    end = time.time()
    print('Script executed in {:.3f} s'.format(end - start))
#%%
