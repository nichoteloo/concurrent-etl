import os
import time
import pandas as pd
import multiprocessing
from multiprocessing import *
from concurrent.futures import ProcessPoolExecutor

os.chdir(r'C:\Users\HP\OneDrive\Desktop\concurrent etl')

# max_worker = os.cpu_count() or 1 ## 12 in my case

lock = Lock()
semTrn = Semaphore(10)
semLd = Semaphore(10)

# extract functionality
def extract():
    return [os.getcwd()+'\\sample\\'+f for f in os.listdir('sample')]

# transform functionality
def transform(file):
    print("process {} transform started ".format(multiprocessing.current_process().pid))
    semTrn.acquire()
    
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

    semTrn.release()
    print("process {} transform completion ".format(multiprocessing.current_process().pid))

    print("process {} load started".format(multiprocessing.current_process().pid))
    semLd.acquire()

    load(df, filename)

# load functionality
def load(tdf, file):
    tdf.to_csv(f'result/multiprocess/{file}',mode='a',header=False,index=False)
    semLd.release()
    print("process {} load completion ".format(multiprocessing.current_process().pid))

# main program
def main():
    executor, lst = ProcessPoolExecutor(max_workers=10), list()

    for file in extract():
        lst.append(executor.submit(transform, file))
    for future in lst:
        future.result()
    
    executor.shutdown()

if __name__ == "__main__":
    st = time.time()
    main() # 21.67 sec best condition (cuma VSCODE doang apps yg kebuka).
    end = time.time() - st
    print("Execution time {} sec".format(end))