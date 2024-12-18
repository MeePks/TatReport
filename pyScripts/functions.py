from sqlalchemy import create_engine

#defining functions for sql alchemy
def open_alchemy_conn(srvname,dbname):
    try:
        conn_string=f'mssql+pyodbc://@{srvname}/{dbname}?driver=ODBC+Driver+17+for+SQL+server&Trusted_connection=yes'
        engine=create_engine(conn_string)
        print(f"Connected to {srvname}:{dbname}")
        return engine
    except:
        print(f"Error Connecting to {srvname}")

def close_alchemy_conn(conn):
    try:
        conn.dispose()
        print(f"Disconnected")
    except:
        print("Error disconnecting from ther server")