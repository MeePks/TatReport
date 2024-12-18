from configparser import ConfigParser
import functions as fn
import pandas as pd

#reading configuration file
config=ConfigParser()
config.read('.\pyScripts\config.ini')
centralized_server=config['sqlconn']['centralized_server']
centralized_db=config['sqlconn']['centralized_db']

#connecting to the centralized server in which there is list of all the servers
centralized_conn=fn.open_alchemy_conn(centralized_server,centralized_db)
df_server_list=pd.read_sql_table('SSIS_ConfigurationInfo',centralized_conn)
centralized_conn.dispose()

#query to check if a stored proc exists or not
query_proc_exists=r"SELECT COUNT(*) AS ProcExist FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '__getTat' AND ROUTINE_TYPE = 'PROCEDURE'"

# Initialize an empty list to store the results
results = []
for index,rows in df_server_list.iterrows():
    if rows['ActiveFlag']:
       audit_conn=fn.open_alchemy_conn(rows['SourceServerName'],rows['InventoryLogDB'])
       df_proc=pd.read_sql_query(query_proc_exists,audit_conn,index_col=None)
       results.append({
           'AuditName':rows['AuditName'],
           'ServerName':rows['SourceServerName'],
           'DatabaseName':rows['InventoryLogDB'],
           'ProcExists':df_proc['ProcExist'][0]
           })
df_proc_exists_list=pd.DataFrame(results)
#df_proc_exists_list.to_csv('TatProcExistlist.csv',index=False)

