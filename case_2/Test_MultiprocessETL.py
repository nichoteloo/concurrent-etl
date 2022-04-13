# not fix yet ....

import os
import json
import time
import urllib
import numpy as np
import pandas as pd
import threading
import multiprocessing
from queue import Queue
from multiprocessing import *
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor

os.chdir(r'D:\concurrent-etl\case_2')

# max_worker = os.cpu_count() or 1 ## 12 in my case
# Benchmarking result:
    # - without parallel: 8 files 1140.42 sec
    # - with parallel: 8 files 724.01 sec

# lock = Lock()
# semTrn = Semaphore(10)
# semLd = Semaphore(10)

q = Queue()
CONFIG_DIRECTORY = './transform_load_config.json'

# read config
def read_config():
    global SAP_CONFIG

    try:
        with open(CONFIG_DIRECTORY) as config_file:
            SAP_CONFIG = json.load(config_file)
    except Exception as argument:
        print(argument)

# connect to mssql
def connect_mssql():
    global engine, connection

    read_config()  

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
    except Exception as argument:
        print(argument)

# update master table
def update_master_table(df, table_name, schema, inserted_column, column_to_join, rename_column):
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

# ======================================================================================
# extract functionality
def extract():
    files = [os.getcwd()+'\\sample\\'+f for f in os.listdir('sample')]
    # q.put(files)
    return files

# transform functionality
def transform(file):
    # print("process {} transform started ".format(multiprocessing.current_process().pid))
    # semTrn.acquire()
    
    filename = (file.split('\\')[-1]).split('.')[0]
    template = filename.split('_')[-1]
    
    connect_mssql()
    
    if template == 'OPERATIONS':
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
        
        df = pd.read_excel(file)[needed_column]
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
            df[column] = pd.to_datetime(df[column].astype(str)[:10], format='%Y-%m-%d', errors='coerce').astype(str).replace({'NaT' : None})

        # Convert time column type
        time_column = ['actualStartTimeExecution', 'actualFinishTimeExecution']
        for column in time_column:
            # Uniform the date format to %H:%M:%S, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].astype(str)[-8:], format='%H:%M:%S', errors='coerce').dt.time.replace({'NaT' : None})

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
        
        dbo_ProductionOrder['productionOrder'] = dbo_ProductionOrder['productionOrder'].astype(np.int64)
        dbo_Site['site'] = dbo_Site['site'].astype(np.int64)
        dbo_Activity['activity'] = dbo_Activity['activity'].astype(np.int64)

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

    elif template == 'CONFIRMATION':
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

        df = pd.read_excel(file)[needed_column]
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
            df[column] = pd.to_datetime(df[column].astype(str)[:10], format='%Y-%m-%d', errors='coerce').astype(str).replace({'NaT' : None})
        
        # Convert time column type
        time_column = ['postingTime',
                    'executionStartTime', 'executionFinishTime']
        for column in time_column:
            # Uniform the date format to %H:%M:%S, convert again to string, then replace NaT with None
            df[column] = pd.to_datetime(df[column].astype(str)[-8:], format='%H:%M:%S', errors='coerce').dt.time.replace({'NaT' : None})

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

        dbo_ProductionOrder['productionOrder'] = dbo_ProductionOrder['productionOrder'].astype(np.int64)
        dbo_Site['site'] = dbo_Site['site'].astype(np.int64)
        dbo_Activity['activity'] = dbo_Activity['activity'].astype(np.int64)

        # Join all table
        transformed_data = df
        transformed_data = pd.merge(transformed_data, dbo_ProductionOrder, how='left', on='productionOrder')
        transformed_data = pd.merge(transformed_data, dbo_Site, how='left', on='site')
        transformed_data = pd.merge(transformed_data, dbo_Activity, how='left', on='activity')
        transformed_data = pd.merge(transformed_data, dbo_WorkCentre, how='left', on='workCentre')
        
        # Get productionActivityTransactionID from available data
        production_order_ID_unique = transformed_data[['productionOrderID']].drop_duplicates(subset = ['productionOrderID'])
        production_order_ID_unique_joined = '(' + ', '.join(production_order_ID_unique['productionOrderID'].astype(str)) + ')'
        print(production_order_ID_unique_joined)
        query_string = """SELECT DISTINCT
                        ID,
                        productionOrderID, siteID, activityID
                        FROM [TW_Operational].[dbo].[ProductionActivityTransaction]
                        WHERE productionOrderID IN """ + production_order_ID_unique_joined
        print(query_string)
        dbo_ProductionActivityTransaction = pd.DataFrame(connection.execute(query_string).fetchall(),
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
    else:
        print("Template not found...")
        exit()
    
    # q.put([transformed_data, filename, template])
    load(transformed_data, filename, template)

    # semTrn.release()
    # print("process {} transform completion ".format(multiprocessing.current_process().pid))

# load functionality
def load(tdf, file, temp):
    # print("process {} load started".format(multiprocessing.current_process().pid))
    # semLd.acquire()

    if temp == 'OPERATIONS':
        print("upload to temp_operation and merge temp_operation")
        # connection.execute("DELETE FROM [TW_Operational].[dbo].[TempProductionActivityTransaction];")

        # tdf.to_sql('TempProductionActivityTransaction', schema='dbo', con=engine, if_exists='append', index=False, chunksize=10000)
        # temp_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityTransaction];").fetchone()[0]
        # print(temp_loaded_row)

        # # Merge Operation Temporary Table with real Table
        # # Update this table to trigger Store Procedure, sometimes we cannot use EXEC directly using Python, but this is the safest way
        # connection.execute(""" UPDATE [TW_Operational].[trigger].[MergeProductionActivityTransaction]
        #                     SET process = 'EXEC [TW_Operational].[dbo].[MergeProductionActivityTransaction]', run_at = CURRENT_TIMESTAMP;""")
    elif temp == 'CONFIRMATION':
        print("upload to temp_confirmation and merge temp_confirmation")
        # connection.execute("DELETE FROM [TW_Operational].[dbo].[TempProductionActivityConfirmation];")

        # # Load Confirmation Excel File to Temporary Table
        # tdf.to_sql('TempProductionActivityConfirmation', schema='dbo', con=engine, if_exists='append', index=False, chunksize=10000)
        # temp_loaded_row = connection.execute("SELECT COUNT (ID) FROM [TW_Operational].[dbo].[TempProductionActivityConfirmation];").fetchone()[0]
        # print(temp_loaded_row)

        # connection.execute(""" UPDATE [TW_Operational].[trigger].[MergeProductionActivityConfirmation]
        #                     SET process = 'EXEC [TW_Operational].[dbo].[MergeProductionActivityConfirmation]', run_at = CURRENT_TIMESTAMP;""")
    else:
        print("Template not found...")
        exit()

    tdf.to_csv(f'result/{file}.csv',mode='a',header=False,index=False)
    # semLd.release()
    # print("process {} load completion ".format(multiprocessing.current_process().pid))

# # etl aggregation function
# def etl():
#     t1 = threading.Thread(target=extract)
#     t1.start()
#     t1.join()
#     files = q.get()

#     for file in files:
#         t2 = threading.Thread(target=transform,args=(file,))
#         t2.start()
#         t2.join()
#         tl = q.get()

#         t3 = threading.Thread(target=load,args=(tl[0],tl[1],tl[2],))
#         t3.start()
#         t3.join()

# main program
def main():
    # # with multithreading
    # t = threading.Thread(target=etl)
    # t.start()

    # # with parallel
    # executor, lst = ProcessPoolExecutor(max_workers=10), list()
    # for file in extract():
    #     lst.append(executor.submit(transform, file))
    # for future in lst:
    #     future.result()
    # executor.shutdown()

    # without parallel
    for file in extract():
        transform(file=file)

if __name__ == "__main__":
    st = time.time()
    main() 
    end = time.time() - st
    print("Execution time {} sec".format(end))