import os

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def create_app():
    from os import path
    from flask import Flask
    from flask_migrate import Migrate
    app = Flask(__name__, root_path=path.abspath(path.dirname(__file__) + '/..'))
    app.config.from_object(Config)
    db.init_app(app)
    Migrate(app, db)
    init_components(app)
    return app


def init_components(app):
    import logging
    import click
    import requests
    from .instrument import Instrument, update_instruments
    from .account import Portfolio, Position, update_rh_account
    from .m1 import M1Portfolio, update_m1_account
    from .analysis import Quote

    # app.robinhood = requests.Session()
    # json = requests.post('https://api.robinhood.com/oauth2/token/',
    #                      json={'username': os.environ['RH_USERNAME'], 'password': os.environ['RH_PASSWORD'],
    #                            'client_id': os.environ['RH_CLIENT_ID'], 'device_token': os.environ['RH_DEVICE_TOKEN'],
    #                            'expires_in': 86400, 'scope': 'internal', 'grant_type': 'password'}).json()
    # app.robinhood.headers['Authorization'] = f"{json['token_type']} {json['access_token']}"
    app.shell_context_processor(lambda: {
        'db': db,
        # 'rh': app.robinhood,
        'Instrument': Instrument,
        'Portfolio': Portfolio,
        'Position': Position,
        'Quote': Quote,
        'M1Portfolio': M1Portfolio,
    })

    app.cli.command()(
        click.option('--popularity_cutoff', default=300)(
            update_instruments))
    # app.cli.command()(update_rh_account)
    app.cli.command()(update_m1_account)
    app.cli.command()(Quote.usd_cny)
    app.logger.setLevel(logging.INFO)


class Config:
    SQLALCHEMY_DATABASE_URI = 'sqlite:///db/portfolio'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = int(os.environ['SQLALCHEMY_ECHO'])
