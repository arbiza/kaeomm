import pandas as pd
from transactions import Transactions
from statements import StatementsParser
from config import Config
from sources import Source

if __name__ == "__main__":

    pd.set_option('display.max_rows', 100)

    cfg = Config()

    acc = Source('revolut', 'PLN')
    acc.statement_column_mapping('Type', 'type')
    acc.statement_column_mapping('Started Date', 'time')
    acc.statement_column_mapping('Description', 'desc')
    acc.statement_column_mapping('Amount', 'amount')
    acc.statement_column_mapping('Fee', 'fee')

    print(acc.to_json())

    # [print(type(i)) for i in acc._stmt_columns_mapping]
    # [print(key, value) for key, value in acc._stmt_columns_mapping.items()]
    for l in acc._stmt_columns_mapping:
        print(l.items())
