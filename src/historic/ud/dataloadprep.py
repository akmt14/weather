import os
import pandas as pd

def transformer():

    """
    _summary_ : Add State & City name to file as a column.
    """

    path='./data/01_historic/local/'

    columns = ['day', 'month', 'year', 'temp_f']

    for _file in os.listdir(path=path):
        if _file.endswith('.txt'):
            filename = _file.split('.')[0]+'.csv'
            data = []
            with open(path+_file, 'r') as f:
                for line in f:
                    data.append(line.split())
            df = pd.DataFrame(data)
            df.columns = columns
            df["state"] = os.path.basename(_file.split('.')[0].split("_")[0])
            df["city"] = os.path.basename(" ".join(i for i in (_file.split('.')[0].split("_")[1:])))
            df.to_csv(path+filename,sep=",",index=False, header=False)


if __name__ == "__main__":
    transformer()