from configparser import ConfigParser
import functions as fn
import pandas as pd

#reading configuration file
config=ConfigParser()
config.read('.\pyScripts\config.ini')
centralized_server=config['sqlconn']['centralized_server']
centralized_db=config['sqlconn']['centralized_db']
sql_script='ReportGeneration.sql'

#connecting to the centralized server in which there is list of all the servers
centralized_conn=fn.open_alchemy_conn(centralized_server,centralized_db)
df_server_list=pd.read_sql_table('SSIS_ConfigurationInfo',centralized_conn)
centralized_conn.dispose()

#query to check if a stored proc exists or not
query_proc_exists=r"SELECT COUNT(*) AS ProcExist FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '__getTat' AND ROUTINE_TYPE = 'PROCEDURE'"

# Initialize an empty list to store the results
results = []
df_results=pd.DataFrame()

#open sql script to read the query
with open(sql_script,'r') as file:
    report_generate_query=file.read()

#iterating through server list
for index,rows in df_server_list.iterrows():
    #iterate only through active audits
    if rows['ActiveFlag']:
       audit_conn=fn.open_alchemy_conn(rows['SourceServerName'],rows['InventoryLogDB']) #opening connection in audits
       df_proc=pd.read_sql_query(query_proc_exists,audit_conn,index_col=None) #checking if proc exists
       #Getting report with the help of sql script Reportgeneration.sql
       try:
            df_result=pd.read_sql_query(report_generate_query,audit_conn,index_col=None)
            df_result['AuditName']=rows['AuditName']
            df_results=pd.concat([df_results,df_result])
       except:
           print(f'Error executing script for {rows["AuditName"]}')

        #Appending the details of proc exists sql statements
       results.append({
           'AuditName':rows['AuditName'],
           'ServerName':rows['SourceServerName'],
           'DatabaseName':rows['InventoryLogDB'],
           'ProcExists':df_proc['ProcExist'][0]
           })
       
#converting list to dataframe for easy data manipulation      
df_proc_exists_list=pd.DataFrame(results)

#df_proc_exists_list.to_csv('TatProcExistlist.csv',index=False)

#pivoting the table
df_pivot=df_results.pivot_table(
    index='AuditName',
    columns='frequency',
    values=['AvgTAT(Mean)','TATMedian'],
    aggfunc='first'
)

# Flatten the MultiIndex columns (if any)
df_pivot.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df_pivot.columns]

# Reset index for a clean DataFrame
df_pivot = df_pivot.reset_index()

# Display the pivoted DataFrame
df_pivot.to_csv('TatReport.csv')

