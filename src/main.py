import pandas as pd
from transactions import Transactions
from statements import StatementsParser
from config import Config
from sources import Source

from datetime import datetime

if __name__ == "__main__":

    cfg = Config()
    trans = Transactions(cfg)

    src_revolut = Source('revolut', 'PLN')
    src_revolut.statement_column_mapping(['Type'], 'type')
    src_revolut.statement_column_mapping(['Started Date'], 'time')
    src_revolut.statement_column_mapping(['Description'], 'desc')
    src_revolut.statement_column_mapping(['Amount'], 'amount')
    src_revolut.statement_column_mapping(['Fee'], 'fee')

    src_millennium = Source('millennium', 'PLN')
    src_millennium.statement_column_mapping(['Transaction Type'], 'type')
    src_millennium.statement_column_mapping(['Transaction date'], 'time')
    src_millennium.statement_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    src_millennium.statement_column_mapping(['Debits', 'Credits'], 'amount')

    # print(acc.to_json())

    # [print(l['src'], l['dst']) for l in src_revolut._stmt_columns_mapping]
    # [print(l['src'], l['dst']) for l in src_millennium._stmt_columns_mapping]

    df_revolut = src_revolut.statement_parse(
        '../data/statements/revolut-2023.csv')
    df_millennium = src_millennium.statement_parse(
        '../data/statements/millenium-2023.csv')

    trans.add_bulk(df_revolut)
    trans.add_bulk(df_millennium)

    # # pd.set_option('display.max_rows', 100)

    print(trans._df)

    trans._df.to_csv('../data/statements/output.csv')
