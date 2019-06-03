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
    app.logger.info('Started')
    return app


def init_components(app):
    import os
    import click
    import requests
    from .instrument import Instrument, update_instruments
    from .account import Transfer, Portfolio, Order, Position, update_account

    app.robinhood = requests.Session()
    app.robinhood.headers['Authorization'] = os.environ['ROBINHOOD_TOKEN']
    app.shell_context_processor(lambda: {
        'db': db,
        'rh': app.robinhood,
        'Instrument': Instrument,
        'Transfer': Transfer,
        'Portfolio': Portfolio,
        'Order': Order,
        'Position': Position,
    })

    app.cli.command()(
        click.option('--popularity_cutoff', default=300)(
            update_instruments))
    app.cli.command()(update_account)


class Config:
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///db/portfolio'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
