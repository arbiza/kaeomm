# This file is not intended to be used in the program logic. The code written
# here is meant to perform updates, fixes, etc., apart from the program logic.


import pandas as pd
import datetime

from config import Config


def db_columns_update():
    '''
    1. Update 'headers' list below  with the headers expected in the new version
    2. Update the data source in "Config('../data/db')"
    3. Run this file calling this function in main
    4. Update the headers list in the Trasnaction class with the same headers
       used here

    The program might work normally with the new headers structure.
    '''

    cfg = Config('../data/db')

    headers = ['id',
               'time',
               'input',
               'type',
               'source',
               'source_id',
               'desc',
               'amount',
               'fee',
               'total',
               'curr',
               'note',
               'system',
               'category',
               'tags'
               ]

    new_df = pd.DataFrame(columns=headers)
    old_df = pd.read_csv(cfg.db_dir + 'transactions.csv', sep='|')

    for h in headers:
        if h in old_df.columns.values:
            new_df[h] = old_df[h]

    new_df.to_csv(cfg.db_dir + 'transactions.csv',
                  sep='|', index=False)


def migrate_colum(col_a, col_b):
    '''
    Copies the content in column A to column B
    '''

    cfg = Config('../data/db')
    df = pd.read_csv(cfg.db_dir + 'transactions.csv', sep='|')

    df[col_b] = df[col_a]

    df.to_csv(cfg.db_dir + 'transactions.csv',
              sep='|', index=False)


def set_id():
    '''
    Sets a value to the 'id' column if it's empty.
    The id is an integer (the highest id value + 1).
    '''

    cfg = Config('../data/db')
    df = pd.read_csv(cfg.db_dir + 'transactions.csv', sep='|')

    for i in range(0, len(df)):
        if pd.isna(df.loc[i, 'id']) or df.loc[i, 'id'] == 0:
            df.loc[i, 'id'] = df['id'].max() + 1

    df.to_csv(cfg.db_dir + 'transactions.csv',
              sep='|', index=False)


if __name__ == "__main__":
    # db_columns_update()
    # migrate_colum('system_cat', 'system')
    set_id()
