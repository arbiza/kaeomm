import pandas as pd
from transactions import Transactions
from statements import Revolut

if __name__ == "__main__":

    # rvlt = Revolut(df, '../data/statements/revolut-2023.csv')
    # print(rvlt.parse())
    t = Transactions()

    print(t._transactions)
