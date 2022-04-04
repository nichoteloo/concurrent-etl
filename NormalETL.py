import time
import pandas as pd

# extract functionality
def extract(d):
    transform(d)

# transform functionality
def transform(df):
    load(df)

# load functionality
def load(tdf):
    tdf.to_csv('airport_freq_out_normal.csv',mode='a',header=False,index=False)
    return "load complete!"

# main program
def main():
    file = 'airport_freq.csv'
    dtype_dict = {'id': 'int8',
                  'airport_ref': 'int8',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'}
    
    df = pd.read_csv(file, dtype=dtype_dict, low_memory=False)
    chuck_size = int(df.shape[0] / 4)

    # normal etl process
    st = time.time()
    extract(df) # 0.10 sec
    en = time.time() - st
    print("etl 1 execution time {} sec".format(en))

    # explicitly separate etl process for each subset (more fast, 0.08 sec)
    st = time.time()
    for start in range(0, df.shape[0], chuck_size):
        df_subset = df.iloc[start : start+chuck_size]
        extract(df_subset)
    en = time.time() - st
    print("etl 2 execution time {} sec".format(en))

if __name__ == "__main__":
    start = time.time()
    main() # 0.17 sec
    end = time.time() - start
    print("Total execution time {} sec".format(end))