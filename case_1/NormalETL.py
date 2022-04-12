import os
import time
import pandas as pd

os.chdir(r'D:\concurrent-etl\case_1')

# extract functionality
def extract():
    return [os.getcwd()+'\\sample\\'+f for f in os.listdir('sample')]

# transform functionality
def transform(file):
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
  
    load(df, filename)

# load functionality
def load(tdf, file):
    tdf.to_csv(f'result/normal/{file}',mode='a',header=False,index=False)
    return "load complete!"

# main program
def main():
    # explicitly separate etl process for each file
    for file in extract():
        transform(file)

if __name__ == "__main__":
    start = time.time()
    main() # 79.60 sec, best 66.24 sec (cuma VSCODE doang apps yg kebuka)
    end = time.time() - start
    print("Total execution time {} sec".format(end))