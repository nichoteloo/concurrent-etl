from distutils.util import execute
import time
import pandas as pd
import multiprocessing
from multiprocessing import *
from concurrent.futures import ProcessPoolExecutor

lock = Lock()
semTrn = Semaphore(5)
semLd = Semaphore(5)

def extract(file):
    dtype_dict = {'id': 'category',
                  'airport_ref': 'category',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'}
    return pd.read_csv(file, dtype=dtype_dict, low_memory=False)

def transform(df):
    print("process {} transform started ".format(multiprocessing.current_process().pid))
    semTrn.acquire()
    df['ref_code'] = df['airport_ident'].astype(str)+str('***')
    semTrn.release()
    print("process {} transform completion ".format(multiprocessing.current_process().pid))

    semLd.acquire()
    load(df)

def load(tdf):
    print("process {} load started".format(multiprocessing.current_process().pid))
    tdf.to_csv('airport_freq_out_multiprocess.csv', mode='a', header=False, index=False)
    semLd.release()
    print("process {} load completion ".format(multiprocessing.current_process().pid))

def main():
    file = 'airport_freq.csv'
    df = extract(file)
    chunk_size = int(df.shape[0] / 4)

    executor = ProcessPoolExecutor(max_workers=5)
    lst = list()

    for start in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[start: start+chunk_size]
        lst.append(executor.submit(transform, df_subset))
    
    for future in lst:
        future.result()
    
    executor.shutdown()

if __name__ == "__main__":
    st = time.time()
    main()
    end = time.time() - st
    print("Execution time {} sec".format(end))