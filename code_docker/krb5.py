#! /usr/local/bin/python


import subprocess


def do_kinit(uid, realm, pwd):
    """ Function is used to authenticate to the Kerberos
    via login and password.
    uid: str, user login;
    realm: str, realm;
    pwd: str, user password.
    """
    kinit_args = ["/usr/bin/kinit", uid+"@"+realm]
    res = subprocess.run(kinit_args,
                         input=pwd.encode(),
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL
                         ).returncode
    print("Returncode of kinit is ", res)


def do_kinit_w_keytab(uid, realm, keytab=None):
    """ Function is used to authenticate to the Kerberos
    via keytab.
    uid: str, user login;
    realm: str, realm;
    keytab: str, destination of keytab. If None used .ketab in WD.
    """
    if keytab is None:
        keytab = ".keytab"
    kinit_args = ["/usr/bin/kinit", uid+"@"+realm, "-t", keytab]
    res = subprocess.run(kinit_args,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL
                         ).returncode
    print("Returncode of kinit is ", res)


if __name__ == '__main__':
    #import getpass
    #import pyodbc

    #print("Test started.")

    #usr = input("User name: ")
    #pwd = getpass.getpass("User password: ")

    def main1(usr):
        print("Doing connection 1.")
        do_kinit_w_keytab(usr, "OFFICEKIEV.FOZZY.LAN")
        dbc = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=S-KV-CENTER-S27.OFFICEKIEV.FOZZY.LAN;'
            'Trusted_Connection=yes')
        cursor = dbc.cursor()
        cursor.execute("select top 1 * "
                       "from [SalesHub.Dev].[DataHub].[SalesStores]")
        row = cursor.fetchone()
        if not row:
            print("No rows returned.")
        while row:
            print(row)
            row = cursor.fetchone()
        cursor.close()
        dbc.close()

    def main2(usr, pwd):
        print("Doing connection 2.")
        do_kinit(usr, "OFFICEKIEV.FOZZY.LAN", pwd)
        dbc = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=KVCEN15-SQLS004.OFFICEKIEV.FOZZY.LAN\\HEAVY004;'
            'Trusted_Connection=yes')
        cursor = dbc.cursor()
        cursor.execute("update [AOForeCastData].[AO].[SeasonalCoefficients] "
                       "set Alpha1 = 0.16 "
                       "where FilialId = 1991 and lagerid = 17")
        dbc.commit()
        #row = cursor.fetchone()
        #if not row:
        #    print("No rows returned.")
        #while row:
        #    print(row)
        #    row = cursor.fetchone()
        cursor.close()
        dbc.close()

    #main1(usr, pwd)
    #main2(usr, pwd)
    #print("All done.")
