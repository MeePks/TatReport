from configparser import ConfigParser
from sqlalchemy import create_engine

#reading configuration file
config=ConfigParser()
config.read('.\pyScripts\config.ini')
centralized_server=config['sqlconn']['centralized_server']
centralized_db=config['sqlconn']['centralized_db']

#defining conn string for sql alchemy
def open_alchemy_conn(srvname,dbname):
    try:
        conn_string=f'mssql+pyodbc://@{srvname}/{dbname}?driver=ODBC+Driver+17+for+SQL+server&Trusted_connection=yes'
        engine=create_engine(conn_string)
        print(f"Connected to {srvname}:{dbname}")
        return engine
    except:
        print(f"Error Connecting to {srvname}")

cntralized_conn=open_alchemy_conn(centralized_server,centralized_db)



