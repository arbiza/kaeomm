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
    acc.statement_column_mapping('Currency', 'curr')

    # print(acc.to_json())

    [print(l['src'], l['dst']) for l in acc._stmt_columns_mapping]

    df = acc.statement_parse('../data/statements/revolut-2023.csv')

    print(df)

    print('time type: {}'.format(type(df['time'][0])))
