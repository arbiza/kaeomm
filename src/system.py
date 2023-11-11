# This file is not intended to be used in the program logic. The code written
# here is meant to perform updates, fixes, etc., apart from the program logic.


import pandas as pd

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

    headers = ['time',
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
               'system_cat',
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


if __name__ == "__main__":
    db_columns_update()
