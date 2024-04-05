import pandas as pd


df=pd.read_csv(r'C:\Users\kkuna\Downloads\redshift-to-sqlserver\TABLE_ORDER_BOOK.csv')

for i in df.itertuples():
    print(i.Schema,i.Table_names)