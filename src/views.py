import pandas as pd
import json
from datetime import datetime

from flask import Blueprint, render_template, redirect, url_for, request, flash

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


@views.route('/transactions', methods=['GET', 'POST'])
def transactions():

    if request.method == 'POST':
        if request.form['action'] == 'k_trx_edit_save':
            data = request.form
            print("This is 'request.form'")
            print(data)

            t.update(
                index=int(request.form.get('index')),
                time=request.form.get('date') + ' ' + request.form.get('time'),
                type=request.form.get('type'),
                source=request.form.get('src'),
                description=request.form.get('desc'),
                amount=float(request.form.get('amount')),
                fee=float(request.form.get('fee')),
                note=request.form.get('note'),
                category=request.form.get('category'),
                tags=request.form.get('tags'),
                overwrite_tags=True
            )

        elif request.POST['action'] == 'k_trx_spread_save':
            pass
        elif request.POST['action'] == 'k_trx_extend_save':
            pass

    # After processing the POST,PUT requests, it always returns the last search
    df = t.search(
        start_date='2018-07-13',
        end_date='2023-10-31'
    )

    df[['category', 'tags', 'note']] = df[[
        'category', 'tags', 'note']].fillna('')

    return render_template('transactions.html', app_name=cfg.app_name, dataframe=df.head(200))


@views.route('/spread')
def spread():

    index = request.args.get('index')

    df = t.search(index=[int(index)])

    return render_template('transaction_spread.html', app_name=cfg.app_name, index=index, df=df.to_dict(orient='records'))
