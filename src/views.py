import pandas as pd
import json

from flask import Blueprint, render_template, redirect, url_for, request

from config import Config
from sources import Sources
from transactions import Transactions

views = Blueprint(__name__, 'views')


cfg = Config('../data/db')
s = Sources(cfg)
t = Transactions(cfg, s)


@views.route('/')
def root():
    return redirect(url_for('views.home'))


@views.route('/home')
def home():
    return render_template('index.html', app_name=cfg.app_name)


@views.route('/transactions')
def transactions():
    df = t.search(
        start_date='2018-07-13',
        end_date='2023-10-31'
    )

    df[['category', 'tags']] = df[['category', 'tags']].fillna('')

    return render_template('transactions.html', app_name=cfg.app_name, dataframe=df.head(20))


@views.route('/spread')
def spread():

    index = request.args.get('index')

    df = t.search(index=[int(index)])

    return render_template('transaction_spread.html', app_name=cfg.app_name, index=index, df=df.to_dict(orient='records'))
