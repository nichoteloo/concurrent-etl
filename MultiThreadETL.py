import time
import threading
import pandas as pd
from threading import *
from concurrent.futures import ThreadPoolExecutor

lock = Lock()
semTrn = Semaphore(4)
semLd = Semaphore(4) 
pd.set_option('mode.chained_assignment', None)

# extract functionality
def extract(file):
    dtype_dict = {
        'id': 'category',
        'airport_ref': 'category',
        'airport_ident': 'category',
        'type': 'category',
        'description': 'category',
        'frequency_mhz': 'float16'
    }
    return pd.read_csv(file, dtype=dtype_dict, low_memory=False)

# transform functionality
def transform(df):
    semTrn.acquire() # lock transform process
    print("thread {} acquired transform lock".format(threading.currentThread().ident))
    df['ref_code'] = df['airport_ident'].astype(str)+str('***') # basic transform
    semTrn.release() # release transform process

    print("thread {} released tranform lock ".format(threading.currentThread().ident))
    print("thread {} acquired load lock ".format(threading.currentThread().ident))

    semLd.acquire() # lock load process
    load(df) 

# load functionality
def load(tdf):
    tdf.to_csv('airport_freq_out_multithread.csv', mode='a', header=False, index=False)
    semLd.release() # release load process
    print("thread {} released load lock  ".format(threading.currentThread().ident))
    print("thread {} load completion ".format(threading.currentThread().ident))

# main program
def main():
    file = 'airport_freq.csv'
    df = extract(file)
    chunk_size = int(df.shape[0] / 4)

    executor = ThreadPoolExecutor(max_workers=4)

    lst = list()
    for st in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[st : st+chunk_size]
        lst.append(executor.submit(transform, df_subset))
    for future in lst:
        future.result()
    
    executor.shutdown()

if __name__ == "__main__":
    start = time.time()
    main() # kurang lebih sama kyk normal 0.20 sec
    end = time.time() - start
    print("Total execution time {} sec".format(end)) 


