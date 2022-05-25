import os
import math
import json
import sys
import time
import urllib
import psutil
import threading
import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, event
from sqlalchemy.orm import sessionmaker
# import multiprocessing
from queue import Queue
# from multiprocessing import *

pd.options.mode.chained_assignment = None
SAMPLING_TIME = 6
MAX_INSERT_ROW = 1000
CONFIG_DIRECTORY = './transform_load_config.json'

# lock = Lock()
# semTrn = Semaphore()
# semTdn = Semaphore()

# queue_time_ops = Queue()
# queue_time_cnf = Queue()

# ========================================================================
# Create Database Connection
# ========================================================================
# create database connection
def connect_mssql():
    try:
        with open(CONFIG_DIRECTORY) as config_file:
            SAP_CONFIG = json.load(config_file)
    except Exception as argument:
        print("Cant read config")
        print(argument) 

    try:
        sql_server_param = urllib.parse.quote_plus(
            'Driver={SQL Server};'
            'Server='   + SAP_CONFIG['SQL_SERVER']['SERVER']   + ';'
            'Database=' + SAP_CONFIG['SQL_SERVER']['DATABASE'] + ';'
            'UID='      + SAP_CONFIG['SQL_SERVER']['USERNAME'] + ';'
            'PWD='      + SAP_CONFIG['SQL_SERVER']['PASSWORD'] + ';'
        )

        engine = create_engine('mssql+pyodbc:///?odbc_connect=' + sql_server_param)
        connection = engine.connect()

        return engine, connection
    except Exception as argument:
        print("Cant connect to sql server")
        print(argument)
        return None, None


# ========================================================================
# Helper Functions
# ========================================================================
# update master table
def update_master_table(df, table_name, schema, inserted_column, column_to_join, rename_column):
    engine, connection = connect_mssql()

    # Read current master table
    master_table = pd.read_sql_table(table_name, engine, schema)

    # Select base column
    base_column = inserted_column[0]

    # Check if data is not null and is unavailable in database
    check_to_upload = ~df[base_column].isin(master_table[base_column]) & ~df[base_column].isnull()
    
    # Select all unavailable data in dataframe
    new_data = df[check_to_upload][inserted_column]

    # Drop duplicated master data
    new_data_unique = new_data.drop_duplicates(subset = [base_column])

    # If there is new data, push to database and get new master_table
    if not new_data_unique.empty:
        new_data_unique.to_sql(table_name, schema=schema, con=engine, if_exists='append', index=False, chunksize=10000)
        master_table = pd.read_sql_table(table_name, engine, schema)

    # Select column to join and rename it
    master_table = master_table[column_to_join].drop_duplicates(subset = [base_column])
    master_table.columns = rename_column
    return master_table
    
# ========================================================================
# Custom Thread Class
# ========================================================================
# custom thread for preprocess each file
class preprocessThread(threading.Thread):
    def __init__(self, file):
        threading.Thread.__init__(self)
        self.threadID   = threading
        self.file       = file
    def run(self):
        preprocess(self.threadID, self.file) 

# custom thread for transform process each file
class transformThread(threading.Thread):
    def __init__(self, threadID, df, template, file):
        threading.Thread.__init__(self)
        self.threadID   = threadID
        self.df         = df
        self.template   = template
        self.file       = file
    def run(self):
        if self.template == 'OPERATIONS':
            transformOperation(self.threadID, self.df, self.file)
        elif self.template == 'CONFIRMATION':
            transformConfirmation(self.threadID, self.df, self.file)

# custom thread for load process each file
class loadThread(threading.Thread):
    def __init__(self, filename, template, queue):
        threading.Thread.__init__(self)
        self.filename   = filename
        self.template   = template
        self.queue      = queue
    def run(self):
        if self.template == 'OPERATIONS':
            loadOperation(self.filename, self.queue)
        elif self.template == 'CONFIRMATION':
            loadConfirmation(self.filename, self.queue)


# ========================================================================
# Preprocess Function
# ========================================================================
# for each file specific type
def preprocess(threadID, file):
    filename = (file.split('\\')[-1]).split('.')[0]
    template = filename.split('_')[-1]

    # Read excel data
    start_read = time.time()
    df = pd.read_excel(file, na_values=None, dtype=str)
    read_excel_time = round(time.time() - start_read)

    total_data = len(df.index)
    total_thread = math.ceil(total_data/MAX_INSERT_ROW)
    threads = []

    print(f'Start transform load data of {filename}')

    # transfrom process
    start_transform = time.time()
    for i in range(total_thread):
        idx_start = i * MAX_INSERT_ROW
        idx_end = (MAX_INSERT_ROW) + i * MAX_INSERT_ROW 
        if idx_end > total_data : idx_end = total_data

        df_ = df.iloc[idx_start:idx_end,:]

        thread = transformThread(i + 1, df_, template, filename)
        threads.append(thread)
        thread.start()
    
    for t in threads:
        t.join()
    transform_time = round(time.time() - start_transform)

    # load process
    start_load = time.time()
    q = Queue()
    thread = loadThread(filename, template, q)
    thread.start()
    loaded_rows = thread.queue.get()
    thread.join()
    load_time = round(time.time() - start_load)

    time_string = '''
    File is {0}
    Total read time is {1}
    TOtal transform time is {2}
    Total load time is {3}
    Total loaded rows is {4}
    '''.format(filename, read_excel_time, transform_time, load_time, loaded_rows)
    print(time_string)

# ========================================================================
# Operation Template
# ========================================================================
# transform operation
def transformOperation(threadID, df, file):
    engine, connection = connect_mssql()

    if (not engine) or (not connection):
        try:
            connect_mssql()
        except Exception as argument:
            print('cannot connect to SQL Server for TL Operation')
            print(argument)
    else:
        # start_transform = time.time()
        needed_column = ['Order', 'Plant', 'Activity',
                        'LatstStartDateExecutn', 'LatstFinishDateExectn',
                        'ActStartDateExecution', 'ActStartTimeExecution', 'ActFinishDateExecutn', 'ActFinishTimeExecutn',
                        'Work center', 'Work center description', 
                        'Confirmed scrap (MEINH)', 'Confirmed yield (MEINH)', 'Operation Quantity (MEINH)',
                        'System Status',
                        'Standard value 1 (VGE01)', 'Standard value 2 (VGE02)', 'Standard value 4 (VGE04)', 'Standard value 5 (VGE05)',
                        'Queue Time (WRTZE)',
                        'Confirmed activ. 1 (ILE01)', 'Confirmed activ. 2 (ILE02)', 'Confirmed activ. 4 (ILE04)', 'Confirmed activ. 5 (ILE05)']

        database_column = ['productionOrder', 'site', 'activity', 
                            'plannedStartDate', 'plannedFinishDate', 
                            'actualStartDateExecution', 'actualStartTimeExecution', 'actualFinishDateExecution', 'actualFinishTimeExecution',
                            'workCentre', 'workCentreDisplayName', 
                            'confirmedActivityScrapQuantity', 'confirmedYield', 'totalOrderQuantity', 
                            'activityOrderStatus', 
                            'standardLabourTime', 'standardSetupTime', 'standardProcessTime', 'standardReworkTime', 
                            'standardQueueTime', 
                            'actualLabourTime', 'actualSetupTime', 'actualProcessTime', 'actualReworkTime']
        
        # rename columns
        df = df[needed_column]
        df.columns = database_column

        # Convert integer column type
        int_column = ['confirmedActivityScrapQuantity', 'confirmedYield', 'totalOrderQuantity']
        for column in int_column:
            df[column] = df[column].astype(int)

        # Convert float column type
        float_column = ['standardLabourTime','standardSetupTime','standardProcessTime','standardReworkTime',
                        'standardQueueTime',
                        'actualLabourTime','actualSetupTime','actualProcessTime','actualReworkTime']
        for column in float_column:
            df[column] = df[column].astype(float)
        
        # Convert date column type
        date_column = ['plannedStartDate','plannedFinishDate',
                    'actualStartDateExecution','actualFinishDateExecution']
        for column in date_column:
            # Uniform the date format to %Y-%m-%d, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].str[:10], format='%Y-%m-%d', errors='coerce').astype(str).replace({'NaT' : None})

        # Convert time column type
        time_column = ['actualStartTimeExecution', 'actualFinishTimeExecution']
        for column in time_column:
            # Uniform the date format to %H:%M:%S, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].str[-8:], format='%H:%M:%S', errors='coerce').dt.time.replace({'NaT' : None})

        # Convert WorkCentre
        df['workCentre'] = df['workCentre'].str[:-3]

        # Update all Master Table
        dbo_ProductionOrder = update_master_table(df, table_name='ProductionOrder', schema='dbo',
                                                inserted_column=['productionOrder'],
                                                column_to_join = ['ID', 'productionOrder'],
                                                rename_column = ['productionOrderID', 'productionOrder'])
        dbo_Site            = update_master_table(df, table_name='Site', schema='dbo',
                                                inserted_column=['site'],
                                                column_to_join = ['ID', 'site'],
                                                rename_column = ['siteID', 'site'])
        dbo_Activity        = update_master_table(df, table_name='Activity', schema='dbo',
                                                inserted_column=['activity'],
                                                column_to_join = ['ID', 'activity'],
                                                rename_column = ['activityID', 'activity'])
        dbo_WorkCentre      = update_master_table(df, table_name='WorkCentre', schema='dbo',
                                                inserted_column=['workCentre', 'workCentreDisplayName'],
                                                column_to_join = ['ID', 'workCentre'],
                                                rename_column = ['workCentreID', 'workCentre']) 
        
        # dbo_ProductionOrder['productionOrder'] = dbo_ProductionOrder['productionOrder'].astype(np.int64)
        # dbo_Site['site'] = dbo_Site['site'].astype(np.int64)
        # dbo_Activity['activity'] = dbo_Activity['activity'].astype(np.int64)

        # Join all table
        transformed_data = df
        transformed_data = pd.merge(transformed_data, dbo_ProductionOrder, how='left', on='productionOrder')
        transformed_data = pd.merge(transformed_data, dbo_Site, how='left', on='site')
        transformed_data = pd.merge(transformed_data, dbo_Activity, how='left', on='activity')
        transformed_data = pd.merge(transformed_data, dbo_WorkCentre, how='left', on='workCentre')

        # Select needed column
        transformed_data = transformed_data[['productionOrderID', 'siteID', 'activityID',
                    'plannedStartDate', 'plannedFinishDate',
                    'actualStartDateExecution', 'actualStartTimeExecution','actualFinishDateExecution', 'actualFinishTimeExecution',
                    'workCentreID',
                    'confirmedActivityScrapQuantity', 'confirmedYield', 'totalOrderQuantity',
                    'activityOrderStatus',
                    'standardLabourTime','standardSetupTime','standardProcessTime','standardReworkTime',
                    'standardQueueTime',
                    'actualLabourTime','actualSetupTime','actualProcessTime','actualReworkTime']]
        # transform_time = round(time.time() - start_transform)
        # print(f'Total transform excel time of {file} is {transform_time}')

        # start_load = time.time()
        # load to temp table
        # transformed_data.to_sql('TempProductionActivityTransaction', schema='dbo', con=engine, if_exists='append', index=False, chunksize=MAX_INSERT_ROW)

        # just for checking
        transformed_data.to_csv(f'result/{file}.csv',mode='a',header=True,index=False)
        # load_time = round(time.time() - start_load)

        # print(f'Total load excel time of {file} is {load_time}')

        # queue_time_ops.put([file, transform_time, load_time])

        print(f'{threadID} finish transform load data operation of {file}')

# load operation
def loadOperation(filename, que):
    # df = pd.read_csv(f'result/{filename}.csv')
    engine, connection = connect_mssql()

    # df.to_sql('TempProductionActivityTransaction', schema='dbo', con=engine, if_exists='append', index=False, chunksize=MAX_INSERT_ROW)
    # temp_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityTransaction];").fetchone()[0]
    temp_loaded_row = 20
    que.put(temp_loaded_row)



# ========================================================================
# Confirmation Template
# ========================================================================
# transform confirmation
def transformConfirmation(threadID, df, file):
    engine, connection = connect_mssql()

    if (not engine) or (not connection):
        try:
            connect_mssql()
        except Exception as argument:
            print('cannot connect to SQL Server for TL Confirmation')
            print(argument)
    else:        
        Session = sessionmaker(engine)
        session = Session()

        # start_transform = time.time()
        needed_column = ['Order', 'Plant',
                        'Posting Date', 'Time',
                        'Activity', 'Work Center', 'Operation Quantity (MEINH)', 'Personnel number',
                        'Confirmed Yield (GMEIN)', 'Confirmed scrap (MEINH)', 
                        'Entered by',
                        'Confirmation', 'Confirm. counter',
                        'Activity to conf. 1 (ILE01)','Activity to conf. 2 (ILE02)','Activity to conf. 4 (ILE04)','Activity to conf. 5 (ILE05)',
                        'Start execution (date)', 'Start execution (time)', 'Fin. Execution (Date)', 'Fin. Execution (Time)']

        database_column = ['productionOrder', 'site',
                            'postingDate', 'postingTime',
                            'activity', 'workCentre', 'operationQuantity', 'personelNumber',
                            'confirmYield', 'confirmScrap',
                            'enterBy',
                            'confirmation', 'confirmCounter',
                            'labourTime', 'setupTime', 'processTime', 'reworkTime',
                            'executionStartDate', 'executionStartTime', 'executionFinishDate', 'executionFinishTime']

        # rename columns
        df = df[needed_column]
        df.columns = database_column

        # Convert integer column type
        int_column = ['operationQuantity', 'confirmYield', 'confirmScrap', 'confirmCounter']
        for column in int_column:
            df[column] = df[column].astype(int)

        # Convert float column type
        float_column = ['labourTime', 'setupTime', 'processTime', 'reworkTime']
        for column in float_column:
            df[column] = df[column].astype(float)
        
        # Convert date column type
        date_column = ['postingDate',
                    'executionStartDate','executionFinishDate']
        for column in date_column:
            # Uniform the date format to %Y-%m-%d, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].str[:10], format='%Y-%m-%d', errors='coerce').astype(str).replace({'NaT' : None})
        
        # Convert time column type
        time_column = ['postingTime',
                    'executionStartTime', 'executionFinishTime']
        for column in time_column:
            # Uniform the date format to %H:%M:%S, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].str[-8:], format='%H:%M:%S', errors='coerce').dt.time.replace({'NaT' : None})

        # Convert WorkCentre
        df['workCentre'] = df['workCentre'].str[:-3]
        
        # Update all Master Table
        dbo_ProductionOrder = update_master_table(df, table_name='ProductionOrder', schema='dbo',
                                                inserted_column=['productionOrder'],
                                                column_to_join = ['ID', 'productionOrder'],
                                                rename_column = ['productionOrderID', 'productionOrder'])
        dbo_Site            = update_master_table(df, table_name='Site', schema='dbo',
                                                inserted_column=['site'],
                                                column_to_join = ['ID', 'site'],
                                                rename_column = ['siteID', 'site'])
        dbo_Activity        = update_master_table(df, table_name='Activity', schema='dbo',
                                                inserted_column=['activity'],
                                                column_to_join = ['ID', 'activity'],
                                                rename_column = ['activityID', 'activity'])
        dbo_WorkCentre      = update_master_table(df, table_name='WorkCentre', schema='dbo',
                                                inserted_column=['workCentre'],
                                                column_to_join = ['ID', 'workCentre'],
                                                rename_column = ['workCentreID', 'workCentre'])

        # dbo_ProductionOrder['productionOrder'] = dbo_ProductionOrder['productionOrder'].astype(np.int64)
        # dbo_Site['site'] = dbo_Site['site'].astype(np.int64)
        # dbo_Activity['activity'] = dbo_Activity['activity'].astype(np.int64)

        # Join all table
        transformed_data = df
        transformed_data = pd.merge(transformed_data, dbo_ProductionOrder, how='left', on='productionOrder')
        transformed_data = pd.merge(transformed_data, dbo_Site, how='left', on='site')
        transformed_data = pd.merge(transformed_data, dbo_Activity, how='left', on='activity')
        transformed_data = pd.merge(transformed_data, dbo_WorkCentre, how='left', on='workCentre')
        
        # Get productionActivityTransactionID from available data
        production_order_ID_unique = transformed_data[['productionOrderID']].drop_duplicates(subset = ['productionOrderID'])
        production_order_ID_unique_joined = '(' + ', '.join(production_order_ID_unique['productionOrderID'].astype(str)) + ')'

        meta = MetaData()
        table_PAT = Table('ProductionActivityTransaction', meta, autoload=True, autoload_with=engine)
        result = session.query(table_PAT).filter(table_PAT.productionOrderID==production_order_ID_unique_joined).all()
        
        # query_string = """SELECT DISTINCT ID, productionOrderID, siteID, activityID
        #                 FROM [TW_Operational].[dbo].[ProductionActivityTransaction]
        #                 WHERE productionOrderID IN """ + production_order_ID_unique_joined
        
        dbo_ProductionActivityTransaction = pd.DataFrame(result,
                                                        columns = ['productionActivityTransactionID',
                                                                    'productionOrderID', 'siteID', 'activityID'])

        transformed_data = pd.merge(transformed_data, dbo_ProductionActivityTransaction, how='left', on=['productionOrderID', 'siteID', 'activityID'])

        # Select needed column
        transformed_data = transformed_data[['productionActivityTransactionID','workCentreID','activityID',
                                            'postingDate', 'postingTime',
                                            'operationQuantity', 'personelNumber',
                                            'confirmYield', 'confirmScrap',
                                            'enterBy',
                                            'confirmation', 'confirmCounter',
                                            'labourTime', 'setupTime', 'processTime', 'reworkTime',
                                            'executionStartDate', 'executionStartTime', 'executionFinishDate', 'executionFinishTime']]
        # transform_time = round(time.time() - start_transform)
        # print(f'Total transform excel time of {file} is {transform_time}')

        # start_load = time.time()
        # load to temp table
        # transformed_data.to_sql('TempProductionActivityConfirmation', schema='dbo', con=engine, if_exists='append', index=False, chunksize=MAX_INSERT_ROW)
        
        transformed_data.to_csv(f'result/{file}.csv',mode='a',header=True,index=False)
        # load_time = round(time.time() - start_load)

        # print(f'Total load excel time of {file} is {load_time}')

        # queue_time_cnf.put([file, transform_time, load_time])

        print(f'{threadID} finish transform load data confirmation of {file}')

# load confirmation
def loadConfirmation(filename, que):
    df = pd.read_csv(f'result/{filename}.csv')
    engine, connection = connect_mssql()

    df.to_sql('TempProductionActivityConfirmation', schema='dbo', con=engine, if_exists='append', index=False, chunksize=MAX_INSERT_ROW)
    temp_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityConfirmation];").fetchone()[0]
    # temp_loaded_row = 50
    que.put(temp_loaded_row)


# ========================================================================
# Main Function
# ========================================================================
if __name__ == "__main__":
    # global engine, connection

    print('Start TL Process')
    p = psutil.Process(os.getpid())
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    _, connection = connect_mssql()

    time_process, count, max_try, threads_ = 0, 0, 3, []
    while True:
        try:
            if time.time() - time_process > SAMPLING_TIME:
                sample_files, filenames, exported_files, fullLoaded = [], [], [], False

                for f in os.listdir('sample'):
                    sample_files.append(os.getcwd()+'\\sample\\'+f)
                    filenames.append((f.split('\\')[-1]).split('.')[0])

                while (not fullLoaded) or (count < max_try):
                    for e in os.listdir('result'):
                        exported_files.append(e.split('.')[0])

                    if set(filenames) == set(exported_files):
                        print("Full loaded. End ETL process. Move to next batch.")
                        fullLoaded = True
                        count = max_try
                    else:
                        count += 1

                    start_time = time.time()

                    # delete temp
                    connection.execute("DELETE FROM [TW_Operational].[dbo].[TempProductionActivityTransaction];")
                    connection.execute("DELETE FROM [TW_Operational].[dbo].[TempProductionActivityConfirmation];")
                    
                    for filepath, filename in zip(sample_files, filenames):
                        if filename not in exported_files:
                            thread = preprocessThread(file=filepath)
                            threads_.append(thread)
                            thread.start()
                    
                    for t in threads_:
                        t.join()

                    # # check updated rows
                    # ops_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityTransaction];").fetchone()[0]
                    # print(f'Total affected rows from Operational insertion is {ops_loaded_row}')
                    # cnf_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityConfirmation];").fetchone()[0]
                    # print(f'Total affected rows from Confirmation insertion is {cnf_loaded_row}')

                    # merge table
                    connection.execute(""" UPDATE [TW_Operational].[trigger].[MergeProductionActivityTransaction]
                                        SET process = 'EXEC [TW_Operational].[dbo].[MergeProductionActivityTransaction]', run_at = CURRENT_TIMESTAMP;""")
                    connection.execute(""" UPDATE [TW_Operational].[trigger].[MergeProductionActivityConfirmation]
                                        SET process = 'EXEC [TW_Operational].[dbo].[MergeProductionActivityConfirmation]', run_at = CURRENT_TIMESTAMP;""")

                    execution_time = time.time() - start_time
                    print(f'Total time taken is {execution_time}')

                time_process = time.time()
        except KeyboardInterrupt:
            print('Exit Process')
            sys.exit(1)
        except Exception as e:
            print(e)