import time
import asyncio
import pandas as pd

pd.set_option('mode.chained_assignment', None)

def extract(file):
    dtype_dict = {'id': 'category',
                  'airport_ref': 'category',
                  'airport_ident': 'category',
                  'type': 'category',
                  'description': 'category',
                  'frequency_mhz': 'float16'}
    return pd.read_csv(file, dtype=dtype_dict,low_memory=False)

async def transform(df):
    df['ref_code'] = df['airport_ident'].astype(str)+str('***')
    await load(df)

async def load(tdf):
    tdf.to_csv('airport_freq_out_asyncio.csv', mode='a', header=False, index=False)
    await asyncio.sleep(0) # biar ngga exit process

async def main():
    file = 'airport_freq.csv'
    df = extract(file)
    chuck_size = int(df.shape[0] / 4)

    for start in range(0, df.shape[0], chuck_size):
        df_subset = df.iloc[start: start+chuck_size]
        x = asyncio.create_task(transform(df_subset))
        await x

st = time.time()
asyncio.run(main()) # 0.17 sec
end = time.time() - st
print("Total execution time {} sec".format(end))

