import os
import time
import threading
import pandas as pd
from threading import *
from concurrent.futures import ThreadPoolExecutor

os.chdir(r'D:\concurrent-etl\case_1')

# max_worker = min(32, (os.cpu_count() or 1) + 4) ## 16 in my case

lock = Lock()
semTrn = Semaphore(10)
semLd = Semaphore(10) 
pd.set_option('mode.chained_assignment', None)

# extract functionality
def extract():
    return [os.getcwd()+'\\sample\\'+f for f in os.listdir('sample')]

# transform functionality
def transform(file):
    semTrn.acquire() # lock transform process
    print("thread {} acquired transform lock".format(threading.currentThread().ident))

    filename = (file.split('\\')[-1]).split('.')[0]
    template = filename.split('_')[-1]

    if template == 'OPERATIONS':
        needed_column = ['Confirmed scrap (MEINH)', 'Confirmed yield (MEINH)', 'Operation Quantity (MEINH)']
        database_column = ['confirmedActivityScrapQuantity', 'confirmedYield', 'totalOrderQuantity']
    elif template == 'CONFIRMATION':
        needed_column = ['Operation Quantity (MEINH)', 'Confirmed Yield (GMEIN)', 'Confirmed scrap (MEINH)', 'Confirm. counter']
        database_column = ['operationQuantity', 'confirmYield', 'confirmScrap', 'confirmCounter']
    else:
        print("Template not found...")
        exit()

    df = pd.read_excel(file)[needed_column]
    df.columns = database_column

    for column in database_column:
        df[column] = df[column].astype(int)

    semTrn.release() # release transform process
    print("thread {} released tranform lock ".format(threading.currentThread().ident))

    print("thread {} acquired load lock ".format(threading.currentThread().ident))
    semLd.acquire() # lock load process

    load(df, filename)

# load functionality
def load(tdf, file):
    tdf.to_csv(f'result/multithread/{file}',mode='a',header=False,index=False)
    semLd.release() # release load process
    print("thread {} released load lock  ".format(threading.currentThread().ident))

# main program
def main():
    executor, lst = ThreadPoolExecutor(max_workers=10), list()
    
    for file in extract():
        lst.append(executor.submit(transform, file))
    for future in lst:
        future.result()
    
    executor.shutdown()

if __name__ == "__main__":
    start = time.time()
    main() # 90.20 sec, sometimes 100.17, depends on cpu. bestnya 81 sec  (cuma VSCODE doang apps yg kebuka).
    end = time.time() - start
    print("Total execution time {} sec".format(end)) 


