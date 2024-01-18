import pandas as pd
from datetime import datetime
import json

from transactions import Transactions
from statements import StatementsParser
from config import Config
from sources import Source
from sources import Sources
from utils import StdReturn


def reset(t: Transactions, sources: Sources) -> None:

    t.reset()
    sources.reset()

    revolut_pln = Source('Revolut PLN', 'PLN', 'UTC')
    revolut_pln.add_stmt_column_mapping(['Type'], 'type')
    revolut_pln.add_stmt_column_mapping(['Started Date'], 'time')
    revolut_pln.add_stmt_column_mapping(['Description'], 'desc')
    revolut_pln.add_stmt_column_mapping(['Amount'], 'amount')
    revolut_pln.add_stmt_column_mapping(['Fee'], 'fee')

    revolut_eur = Source('Revolut EUR', 'EUR', 'UTC')
    revolut_eur.add_stmt_column_mapping(['Type'], 'type')
    revolut_eur.add_stmt_column_mapping(['Started Date'], 'time')
    revolut_eur.add_stmt_column_mapping(['Description'], 'desc')
    revolut_eur.add_stmt_column_mapping(['Amount'], 'amount')
    revolut_eur.add_stmt_column_mapping(['Fee'], 'fee')

    revolut_usd = Source('Revolut USD', 'USD', 'UTC')
    revolut_usd.add_stmt_column_mapping(['Type'], 'type')
    revolut_usd.add_stmt_column_mapping(['Started Date'], 'time')
    revolut_usd.add_stmt_column_mapping(['Description'], 'desc')
    revolut_usd.add_stmt_column_mapping(['Amount'], 'amount')
    revolut_usd.add_stmt_column_mapping(['Fee'], 'fee')

    revolut_gbp = Source('Revolut GBP', 'GBP', 'UTC')
    revolut_gbp.add_stmt_column_mapping(['Type'], 'type')
    revolut_gbp.add_stmt_column_mapping(['Started Date'], 'time')
    revolut_gbp.add_stmt_column_mapping(['Description'], 'desc')
    revolut_gbp.add_stmt_column_mapping(['Amount'], 'amount')
    revolut_gbp.add_stmt_column_mapping(['Fee'], 'fee')

    millennium_pln = Source('Millennium PLN', 'PLN', 'Europe/Warsaw')
    millennium_pln.add_stmt_column_mapping(['Transaction Type'], 'type')
    millennium_pln.add_stmt_column_mapping(['Transaction date'], 'time')
    millennium_pln.add_stmt_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    millennium_pln.add_stmt_column_mapping(['Debits', 'Credits'], 'amount')

    millennium_eur = Source('Millennium EUR', 'EUR', 'Europe/Warsaw')
    millennium_eur.add_stmt_column_mapping(['Transaction Type'], 'type')
    millennium_eur.add_stmt_column_mapping(['Transaction date'], 'time')
    millennium_eur.add_stmt_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    millennium_eur.add_stmt_column_mapping(['Debits', 'Credits'], 'amount')

    millennium_usd = Source('Millennium USD', 'USD', 'Europe/Warsaw')
    millennium_usd.add_stmt_column_mapping(['Transaction Type'], 'type')
    millennium_usd.add_stmt_column_mapping(['Transaction date'], 'time')
    millennium_usd.add_stmt_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    millennium_usd.add_stmt_column_mapping(['Debits', 'Credits'], 'amount')

    millennium_card = Source('Millennium Card', 'PLN', 'Europe/Warsaw')
    millennium_card.add_stmt_column_mapping(['Transaction Type'], 'type')
    millennium_card.add_stmt_column_mapping(['Transaction date'], 'time')
    millennium_card.add_stmt_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    millennium_card.add_stmt_column_mapping(['Debits', 'Credits'], 'amount')

    millennium_savings = Source('Millennium Savings', 'PLN', 'Europe/Warsaw')
    millennium_savings.add_stmt_column_mapping(['Transaction Type'], 'type')
    millennium_savings.add_stmt_column_mapping(['Transaction date'], 'time')
    millennium_savings.add_stmt_column_mapping(
        ['Benefeciary/Sender', 'Description'], 'desc')
    millennium_savings.add_stmt_column_mapping(['Debits', 'Credits'], 'amount')

    pluxee_phy = Source('Pluxee - physical card', 'PLN', 'Europe/Warsaw')

    [sources.add_source(s) for s in [
        revolut_pln, revolut_eur, revolut_usd, revolut_gbp, millennium_pln, millennium_card, millennium_savings, millennium_eur, millennium_usd, pluxee_phy]
     ]

    new_dfs = [
        sources.get_source('Revolut PLN').statement_parse(
            '../data/statements/revolut-pln.csv'),
        sources.get_source('Revolut EUR').statement_parse(
            '../data/statements/revolut-eur.csv'),
        sources.get_source('Revolut USD').statement_parse(
            '../data/statements/revolut-usd.csv'),
        sources.get_source('Revolut GBP').statement_parse(
            '../data/statements/revolut-gbp.csv'),
        sources.get_source('Millennium PLN').statement_parse(
            '../data/statements/millennium-pln.csv'),
        sources.get_source('Millennium EUR').statement_parse(
            '../data/statements/millennium-eur.csv'),
        sources.get_source('Millennium USD').statement_parse(
            '../data/statements/millennium-usd.csv'),
        sources.get_source('Millennium Card').statement_parse(
            '../data/statements/millennium-credit-card.csv'),
        sources.get_source('Millennium Savings').statement_parse(
            '../data/statements/millennium-savings.csv')
    ]

    t.add_bulk(new_dfs)


if __name__ == "__main__":

    cfg = Config('../data/db')
    s = Sources(cfg)
    t = Transactions(cfg, s)

    # reset(t, s)

    # t.update(
    #     search_result=t.search(
    #         # index=[2092],
    #         # start_date='2023-07-13',
    #         # end_date='2023-07-15',
    #         # type='TRADE',
    #         # source='',
    #         # description='jatagan',
    #         # total=,
    #         # currency='',
    #         # note='',
    #         # system_category='',
    #         # categories='',
    #         # tags=['Long-lasting goods']
    #     ),
    #     # description='',
    #     # amount=,
    #     # fee=,
    #     # note='Truflo\'s castration',
    #     # system_category='self transfer',
    #     # category='pet',
    #     # tags=['truflo', 'veterinarian'],
    #     overwrite_tags=True
    # )

    # t.print_to_cli([], 100)

    # with pd.option_context('display.min_rows', 100, 'display.max_rows', 100):
    #     print(
    #         t.search(
    #             # index=[1571],
    #             # start_date='2023-07-13',
    #             # end_date='2023-07-15',
    #             # type='test',
    #             # source='',
    #             # description='benefici',
    #             # total=,
    #             # currency='',
    #             # note='',
    #             # system_category='',
    #             # categories='',
    #             # tags=['Long-lasting goods']
    #         )
    #     )

    # t.link([13, 14, 3158])

    # with pd.option_context('display.min_rows', 100, 'display.max_rows', 100):
    #     print(t.search(link=14))

    # t.allot(3158, 66, 0, 'this is a test', 'alcohol')
    # t.allot(3158, 23.12, 0, 'this is a test', 'gifts')

    # with pd.option_context('display.min_rows', 100, 'display.max_rows', 100):
    #     print(t.search(link=14))

    # print(t.delete([3159]))

    # print('\nAFTER DELETION\n')

    # with pd.option_context('display.min_rows', 100, 'display.max_rows', 100):
    #     print(t.search(link=14))

    # with pd.option_context('display.min_rows', 100, 'display.max_rows', 100):
    #     print(t._df)

    # print(t.search(start_date='2022-12-12'))
    # print(t.search(start_date='2023-08-08'))

    t.add_bulk([s.get_source('Revolut PLN').statement_parse(
        '../data/statements/revolut-pln.csv')])

    print(t._delete_duplicates())

    # temp2 = json.loads(t._df.loc[1, 'system'])

    # print(type(temp2), temp2)

    # t.save()
    # s.save()
    # cfg.save()

    # t.backup()
