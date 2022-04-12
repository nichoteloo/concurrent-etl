import os
import time
import asyncio
import pandas as pd

os.chdir(r'D:\concurrent-etl\case_1')
pd.set_option('mode.chained_assignment', None)

# extract functionality
def extract():
    return [os.getcwd()+'\\sample\\'+f for f in os.listdir('sample')]

# transform functionality
async def transform(file):
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

    await load(df, filename)

# load functionality
async def load(tdf, file):
    tdf.to_csv(f'result/asyncio/{file}',mode='a',header=False,index=False)
    await asyncio.sleep(0) # biar ngga exit process

# main program
async def main():
    for file in extract():
        x = asyncio.create_task(transform(file))
        await x

st = time.time()
asyncio.run(main()) # best nya 66.52 sec (cuma VSCODE doang apps yg kebuka).
end = time.time() - st
print("Total execution time {} sec".format(end))

