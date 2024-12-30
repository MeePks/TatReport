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


#generating overall Report and changeing the order of columns
#df_results_column=['AuditName','Frequency','AvgTAT(Mean)','TATMedian']
#df_results=df_results[df_results_column]
df_results.to_csv('TatReportDetails.csv', index=False)

#pivoting the table
df_pivot=df_results.pivot_table(
    index='AuditName',
    columns='Frequency',
    values=['AvgTAT(Mean)','TATMedian'],
    aggfunc='first'
)

# Flatten the MultiIndex columns (if any)
df_pivot.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df_pivot.columns]

# Reset index for a clean DataFrame
df_pivot = df_pivot.reset_index()

# Display the pivoted DataFrame
df_pivot.to_csv('TatReport.csv',index=False)

#pivoting the table based on Mean
df_pivot=df_results.pivot_table(
    index='AuditName',
    columns='Frequency',
    values=['AvgTAT(Mean)'],
    aggfunc='first'
)

# Remove the multi-level column name (flatten it)
df_pivot.columns.name = None  # Remove the 'frequency' header

# Rename the columns to clean any extra metadata
df_pivot.columns = [col if isinstance(col, str) else col[1].strip() for col in df_pivot.columns]
df_pivot = df_pivot.reset_index()  # Reset the index to keep AuditName as a column

# Display the pivoted DataFrame
df_pivot.to_csv('TatReport_Mean.csv',index=False)

#pivoting the table based on Median
df_pivot=df_results.pivot_table(
    index='AuditName',
    columns='Frequency',
    values='TATMedian',
    aggfunc='first'
)

# Remove the multi-level column name (flatten it)
df_pivot.columns.name = None  # Remove the 'frequency' header


# Rename the columns to clean any extra metadata
df_pivot.columns = [col if isinstance(col, str) else col[1].strip() for col in df_pivot.columns]
df_pivot = df_pivot.reset_index()  # Reset the index to keep AuditName as a column

# Display the pivoted DataFrame
df_pivot.to_csv('TatReport_Median.csv',index=False)
