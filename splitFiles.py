import pandas as pd

def total_lines(file):
    return sum(1 for line in (open(file,encoding='utf-8')))

def split_file(file):
    nb = total_lines(file)
    threads = 4
    split_nb = int(nb/threads)
    count = 0 

    for i in range(1, nb, split_nb):
        df = pd.read_csv(file,
                         header=None,
                         nrows = split_nb,
                         skiprows=i,low_memory=False)
        out_file = 'airport_free_'+str(count)+'.csv'
        df.to_csv(out_file,index=False,header=["id","airport_ref","airport_ident","type","description","frequency_mhz"],mode='a',chunksize=split_nb)
        count+=1
        
def main():
    file = 'airport_free.csv'
    split_file(file)

if __name__ == "__main__":
    main()