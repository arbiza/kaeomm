import pandas as pd
from transactions import Transactions
from statements import StatementsParser
from config import Config
from sources import Source
from sources import Sources

from datetime import datetime
import pytz
import tzlocal


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

    [sources.add_source(s) for s in [
        revolut_pln, revolut_eur, revolut_usd, revolut_gbp, millennium_pln, millennium_card, millennium_savings, millennium_eur, millennium_usd]
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
    t = Transactions(cfg)

    s = Sources(cfg)

    # reset(t, s)

    # t.search_n_add_category_tag('desc', 'Dominika', 'home', ['rent'])
    # t.search_n_add_category_tag(
    #     'desc', 'Biedronka', 'groceries', ['alcohol', 'food'])
    # t.search_n_add_category_tag('desc', 'UPC Polska', 'services', [
    #                             'internet', 'essential'])

    t.print_to_cli([], 20)
    t.save()
    s.save()
