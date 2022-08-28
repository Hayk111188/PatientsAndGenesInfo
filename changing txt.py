
import pandas as pd
import s3fs
path = 'C://Python projects//MySQL genetic database//cnv_processed'
df = pd.read_csv(path + '.txt' , delim_whitespace=True)
df.to_excel(path + '.xlsx' , index = False)
