import pandas as pd

from flask import Blueprint, render_template, redirect, url_for

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

    # return render_template('transactions.html', app_name=cfg.app_name, dataframe=df[cfg.public_headers()].tail().to_html(classes='text-start table table-borderless table-striped table-hover table-responsive align-middle'))

    return render_template('transactions.html', app_name=cfg.app_name, dataframe=df, headers=cfg.headers())
