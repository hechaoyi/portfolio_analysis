from datetime import datetime, date

from sqlalchemy import func

from app.instrument import Instrument
from . import db


class Transfer(db.Model):
    id = db.Column(db.String(40), primary_key=True)
    created_at = db.Column(db.DateTime, nullable=False)
    amount = db.Column(db.Float, nullable=False)

    def __str__(self):
        return f'[{self.created_at}] {self.amount}'

    @classmethod
    def create_or_update(cls, id, created_at, direction, amount):
        inst = cls.query.get(id)
        if not inst:
            inst = cls(id=id)
            db.session.add(inst)
        inst.created_at = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
        assert direction == 'deposit'
        inst.amount = float(amount)
        return inst


class Portfolio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    cost = db.Column(db.Float, nullable=False)
    equity = db.Column(db.Float, nullable=False)
    stocks_value = db.Column(db.Float, nullable=False)
    coins_value = db.Column(db.Float, nullable=False)
    cash_value = db.Column(db.Float, nullable=False)
    today_return_pct = db.Column(db.Float, nullable=False)
    total_return_pct = db.Column(db.Float, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False)
    # relationship
    previous_id = db.Column(db.Integer, db.ForeignKey('portfolio.id'))
    previous = db.relationship('Portfolio', remote_side=[id], backref='next')

    def __str__(self):
        return f'[{self.date}] {self.equity} {self.today_return_pct}%/{self.total_return_pct}%'

    @classmethod
    def create_or_update(cls, cost):
        today, rh = date.today(), db.get_app().robinhood
        inst = cls.query.filter_by(date=today).first()
        if not inst:
            previous = cls.query.filter(cls.date < today).order_by(cls.date.desc()).first()
            inst = cls(date=today, previous=previous)
            db.session.add(inst)
        inst.cost = cost
        json = rh.get('https://api.robinhood.com/portfolios/').json()
        inst.stocks_value = float(json['results'][0]['market_value'])
        inst.cash_value = round(float(json['results'][0]['equity']) - inst.stocks_value, 2)
        json = rh.get('https://nummus.robinhood.com/portfolios/').json()
        inst.coins_value = float(json['results'][0]['extended_hours_market_value'])
        inst.equity = round(inst.stocks_value + inst.coins_value + inst.cash_value, 2)
        if inst.equity > 0:
            prev_equity, prev_cost = (inst.previous.equity, inst.previous.cost) if inst.previous else (0, 0)
            inst.today_return_pct = round(inst.equity / (prev_equity + inst.cost - prev_cost) * 100 - 100, 2)
            inst.total_return_pct = round(inst.equity / inst.cost * 100 - 100, 2)
        else:
            inst.today_return_pct, inst.total_return_pct = 0, 0
        inst.last_update = datetime.utcnow()
        return inst


class Order(db.Model):
    id = db.Column(db.String(40), primary_key=True)
    symbol = db.Column(db.String(8), db.ForeignKey('instrument.symbol'), nullable=False)
    instrument = db.relationship('Instrument')
    amount = db.Column(db.Float, nullable=False)
    executed_at = db.Column(db.DateTime, nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    fees = db.Column(db.Float, nullable=False)
    # relationship
    position_id = db.Column(db.Integer, db.ForeignKey('position.id'))
    position = db.relationship('Position', backref='orders')

    def __str__(self):
        return f'[{self.created_at}] {self.amount}'

    @classmethod
    def create_or_update(cls, id, instrument, executed_at, price, quantity, fees, side):
        inst = cls.query.get(id)
        if not inst:
            inst = cls(id=id)
            inst.instrument = instrument
            db.session.add(inst)
        inst.executed_at = datetime.strptime(executed_at, '%Y-%m-%dT%H:%M:%S.%fZ')
        inst.price = float(price)
        inst.quantity = float(quantity)
        inst.fees = float(fees)
        sign = +1 if side == 'buy' else -1
        inst.amount = round(sign * inst.price * inst.quantity + inst.fees, 2)
        return inst


class Position(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(8), db.ForeignKey('instrument.symbol'), nullable=False)
    instrument = db.relationship('Instrument')
    date = db.Column(db.Date, nullable=False)
    cost = db.Column(db.Float, nullable=False)
    equity = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    avg_buy_price = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float, nullable=False)
    today_return_pct = db.Column(db.Float, nullable=False)
    total_return_pct = db.Column(db.Float, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False)
    # relationship
    previous_id = db.Column(db.Integer, db.ForeignKey('position.id'))
    previous = db.relationship('Position', remote_side=[id], backref='next')
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolio.id'), nullable=False)
    portfolio = db.relationship('Portfolio', backref='positions')

    def __str__(self):
        return f'[{self.date}] {self.equity} {self.today_return_pct}%/{self.total_return_pct}%'

    @classmethod
    def create_or_update(cls, instrument, previous, portfolio, quantity):
        orders = Order.query.filter_by(instrument=instrument, position=None).all()
        today = date.today()
        inst = cls.query.filter_by(instrument=instrument, date=today).first()
        if not inst:
            if previous and previous.quantity == 0:
                previous = None
            inst = cls(instrument=instrument, date=today, previous=previous, portfolio=portfolio)
            db.session.add(inst)
        inst.orders.extend(orders)
        inst.quantity = float(quantity)
        inst.cost = (inst.previous.cost if inst.previous else 0) + sum(o.amount for o in inst.orders)
        inst.avg_buy_price = round(inst.cost / inst.quantity, 2) if inst.quantity > 0 else 0
        inst.current_price = instrument.price
        inst.equity = round(inst.current_price * inst.quantity, 2)
        if inst.equity > 0:
            prev_equity, prev_cost = (inst.previous.equity, inst.previous.cost) if inst.previous else (0, 0)
            inst.today_return_pct = round(inst.equity / (prev_equity + inst.cost - prev_cost) * 100 - 100, 2)
            inst.total_return_pct = round(inst.equity / inst.cost * 100 - 100, 2)
        else:
            inst.today_return_pct, inst.total_return_pct = 0, 0
        inst.last_update = datetime.utcnow()
        return inst


def update_account():
    rh, logger = db.get_app().robinhood, db.get_app().logger

    # Transfers
    for trans in reversed(rh.get('https://api.robinhood.com/ach/transfers/').json()['results']):
        Transfer.create_or_update(trans['id'], trans['created_at'], trans['direction'], trans['amount'])

    # Portfolio
    cost = db.session.query(func.sum(Transfer.amount)).scalar()
    portfolio = Portfolio.create_or_update(cost)
    logger.info('%s', portfolio)

    # Orders
    for order in reversed(rh.get('https://api.robinhood.com/orders/').json()['results']):
        if order['state'] == 'filled':
            s = order['instrument'][len('https://api.robinhood.com/instruments/'):-1]
            instrument = Instrument.query.filter_by(robinhood_id=s).first()
            Order.create_or_update(order['id'], instrument,
                                   order['executions'][0]['timestamp'],
                                   order['executions'][0]['price'],
                                   order['executions'][0]['quantity'],
                                   order['fees'], order['side'])

    # Positions
    previous_positions = {pos.symbol: pos for pos in portfolio.previous.positions} if portfolio.previous else {}
    for pos in rh.get('https://api.robinhood.com/positions/?nonzero=true').json()['results']:
        s = pos['instrument'][len('https://api.robinhood.com/instruments/'):-1]
        instrument = Instrument.query.filter_by(robinhood_id=s).first()
        previous = previous_positions.pop(instrument.symbol, None)
        logger.info('%s', Position.create_or_update(instrument, previous, portfolio, pos['quantity']))
    if previous_positions:
        for prev in previous_positions.values():
            logger.info('%s', Position.create_or_update(prev.instrument, prev, portfolio, 0))
    db.session.commit()
