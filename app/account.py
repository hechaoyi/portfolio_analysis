import os
from datetime import datetime, date
from statistics import mean

from sqlalchemy import func

from app.instrument import Instrument
from . import db

MARGIN_LIMIT = int(os.environ['MARGIN_LIMIT'])


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
        inst.created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
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
    today_return_pct = db.Column(db.Float)
    total_return_pct = db.Column(db.Float)
    last_update = db.Column(db.DateTime)
    # relationship
    previous_id = db.Column(db.Integer, db.ForeignKey('portfolio.id'))
    previous = db.relationship('Portfolio', remote_side=[id], backref='next')

    def __str__(self):
        return f'[{self.date}] Portfolio' \
            f' | {self.equity} ({self.equity - self.cost_today:+.2f})' \
            f' | {self.today_return_pct}%/{self.total_return_pct}%'

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
        # json['results'][0]['extended_hours_market_value']
        inst.stocks_value = float(json['results'][0]['market_value'])
        # json['results'][0]['extended_hours_equity']
        inst.cash_value = round(float(json['results'][0]['equity']) - inst.stocks_value, 2)
        json = rh.get('https://nummus.robinhood.com/portfolios/').json()
        inst.coins_value = float(json['results'][0]['extended_hours_market_value']
                                 or json['results'][0]['market_value'])
        inst.equity = round(inst.stocks_value + inst.coins_value + inst.cash_value, 2)
        inst.today_return_pct = round((inst.equity - inst.cost_today) /
                                      (inst.cost_today if inst.equity > 0 else
                                       (inst.previous.equity if inst.previous else 0)) * 100, 2)
        inst.total_return_pct = round((inst.equity - inst.cost) / mean(inst.cost_timeline()) * 100, 2)
        inst.last_update = datetime.utcnow()
        return inst

    @property
    def cost_today(self):
        prev_equity, prev_cost = (self.previous.equity, self.previous.cost) if self.previous else (0, 0)
        return prev_equity + self.cost - prev_cost

    def cost_timeline(self):
        cur = self
        while cur:
            if cur.equity > 0:
                yield cur.cost
            cur = cur.previous

    def calculate_3xetf(self):
        tmf = next((pos for pos in self.positions if pos.symbol == 'TMF'), None)
        upro = next((pos for pos in self.positions if pos.symbol == 'UPRO'), None)
        if not tmf or not upro:
            return None
        equity, cost_today = tmf.equity + upro.equity, tmf.cost_today + upro.cost_today
        today_return_pct = (equity - cost_today) / cost_today
        total_return_pct = (equity - tmf.cost - upro.cost) / (mean(tmf.cost_timeline()) + mean(upro.cost_timeline()))
        return f'{today_return_pct * 100:+.2f}%/{total_return_pct * 100:+.2f}%'
        # p = Portfolio.query.get(18)
        # while p:
        #     print(p.date, p.calculate_3xetf())
        #     p = p.previous


class PositionSetting(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(8), db.ForeignKey('instrument.symbol'), nullable=False)
    instrument = db.relationship('Instrument')
    proportion = db.Column(db.Float, nullable=False)
    drawdown = db.Column(db.Float)
    profit_val = db.Column(db.Float)
    return_pct = db.Column(db.Float)

    def __str__(self):
        return f'{self.symbol} {self.proportion}%'


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
        return f'[{self.executed_at}] {self.symbol} {self.amount}'

    @classmethod
    def create_or_update(cls, id, instrument, executed_at, price, quantity, fees, side):
        inst = cls.query.get(id)
        if not inst:
            inst = cls(id=id)
            inst.instrument = instrument
            db.session.add(inst)
        inst.executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
        inst.price = float(price)
        inst.quantity = float(quantity)
        inst.fees = float(fees)
        sign = +1 if side == 'buy' else -1
        inst.amount = round(sign * inst.price * inst.quantity + inst.fees, 2)
        return inst


class Dividend(db.Model):
    id = db.Column(db.String(40), primary_key=True)
    symbol = db.Column(db.String(8), db.ForeignKey('instrument.symbol'), nullable=False)
    instrument = db.relationship('Instrument')
    amount = db.Column(db.Float, nullable=False)
    executed_at = db.Column(db.DateTime, nullable=False)
    rate = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    # relationship
    position_id = db.Column(db.Integer, db.ForeignKey('position.id'))
    position = db.relationship('Position', backref='dividends')

    def __str__(self):
        return f'[{self.executed_at}] {self.symbol} {self.amount}'

    @classmethod
    def create_or_update(cls, id, instrument, amount, executed_at, rate, quantity):
        inst = cls.query.get(id)
        if not inst:
            inst = cls(id=id)
            inst.instrument = instrument
            db.session.add(inst)
        inst.amount = float(amount)
        inst.executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
        inst.rate = float(rate)
        inst.quantity = float(quantity)
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
    today_return_pct = db.Column(db.Float)
    total_return_pct = db.Column(db.Float)
    last_update = db.Column(db.DateTime)
    # relationship
    previous_id = db.Column(db.Integer, db.ForeignKey('position.id'))
    previous = db.relationship('Position', remote_side=[id], backref='next')
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolio.id'), nullable=False)
    portfolio = db.relationship('Portfolio', backref='positions')

    def __str__(self):
        return f'[{self.date}] {self.symbol} {self.percentage_of_portfolio}%' \
            f' | {self.equity} ({self.equity - self.cost_today:+.2f})' \
            f' | {self.today_return_pct}%/{self.total_return_pct}%'

    @classmethod
    def create_or_update(cls, instrument, previous, portfolio, quantity):
        orders = Order.query.filter_by(instrument=instrument, position=None).all()
        dividends = Dividend.query.filter_by(instrument=instrument, position=None).all()
        today = date.today()
        inst = cls.query.filter_by(instrument=instrument, date=today).first()
        if not inst:
            if previous and previous.quantity == 0:
                previous = None
            inst = cls(instrument=instrument, date=today, previous=previous, portfolio=portfolio)
            db.session.add(inst)
        inst.orders.extend(orders)
        inst.dividends.extend(dividends)
        inst.quantity = float(quantity)
        inst.cost = round((inst.previous.cost if inst.previous else 0)
                          + sum(o.amount for o in inst.orders)
                          - sum(o.amount for o in inst.dividends), 2)
        inst.avg_buy_price = round(inst.cost / inst.quantity, 2) if inst.quantity > 0 else 0
        inst.current_price = instrument.price
        inst.equity = round(inst.current_price * inst.quantity, 2)
        inst.today_return_pct = round((inst.equity - inst.cost_today) /
                                      (inst.cost_today if inst.equity > 0 else
                                       (inst.previous.equity if inst.previous else 0)) * 100, 2)
        inst.total_return_pct = round((inst.equity - inst.cost) / mean(inst.cost_timeline()) * 100, 2)
        inst.last_update = datetime.utcnow()
        return inst

    @property
    def cost_today(self):
        prev_equity, prev_cost = (self.previous.equity, self.previous.cost) if self.previous else (0, 0)
        return prev_equity + self.cost - prev_cost

    def cost_timeline(self):
        cur = self
        while cur:
            if cur.equity > 0:
                yield cur.cost
            cur = cur.previous

    @property
    def percentage_of_portfolio(self):
        # return round(self.equity / (self.portfolio.stocks_value + self.portfolio.coins_value) * 100, 2)
        return round(self.equity / (self.portfolio.equity + MARGIN_LIMIT) * 100, 2)


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
    for order in reversed(rh.get('https://nummus.robinhood.com/orders/').json()['results']):
        if order['state'] == 'filled':
            Order.create_or_update(order['id'], Instrument.query.get('BTC'),
                                   order['executions'][0]['timestamp'],
                                   order['executions'][0]['effective_price'],
                                   order['executions'][0]['quantity'],
                                   0, order['side'])
    for order in reversed(rh.get('https://api.robinhood.com/orders/').json()['results']):
        if order['state'] == 'filled':
            s = order['instrument'][len('https://api.robinhood.com/instruments/'):-1]
            instrument = Instrument.query.filter_by(robinhood_id=s).first()
            Order.create_or_update(order['id'], instrument,
                                   order['executions'][0]['timestamp'],
                                   order['executions'][0]['price'],
                                   order['executions'][0]['quantity'],
                                   order['fees'], order['side'])

    # Dividends
    for dividend in reversed(rh.get('https://api.robinhood.com/dividends/').json()['results']):
        if dividend['state'] == 'paid':
            s = dividend['instrument'][len('https://api.robinhood.com/instruments/'):-1]
            instrument = Instrument.query.filter_by(robinhood_id=s).first()
            Dividend.create_or_update(dividend['id'], instrument,
                                      dividend['amount'], dividend['paid_at'],
                                      dividend['rate'], dividend['position'])

    # Positions
    previous_positions = {pos.symbol: pos for pos in portfolio.previous.positions} if portfolio.previous else {}
    quantity = rh.get('https://nummus.robinhood.com/holdings/').json()['results'][0]['quantity']
    instrument, previous = Instrument.query.get('BTC'), previous_positions.pop('BTC', None)
    logger.info('%s', Position.create_or_update(instrument, previous, portfolio, quantity))
    for pos in rh.get('https://api.robinhood.com/positions/?nonzero=true').json()['results']:
        if float(pos['quantity']) > 0:
            s = pos['instrument'][len('https://api.robinhood.com/instruments/'):-1]
            instrument = Instrument.query.filter_by(robinhood_id=s).first()
            previous = previous_positions.pop(instrument.symbol, None)
            logger.info('%s', Position.create_or_update(instrument, previous, portfolio, pos['quantity']))
    if previous_positions:
        for prev in previous_positions.values():
            if prev.quantity > 0:
                logger.info('%s', Position.create_or_update(prev.instrument, prev, portfolio, 0))

    # 3xETF
    logger.info('3xETF: %s', portfolio.calculate_3xetf())

    # Recommendations
    positions = {pos.symbol: pos for pos in portfolio.positions}
    for setting in PositionSetting.query.all():
        pos = positions.pop(setting.symbol, None)
        diff = (portfolio.equity + MARGIN_LIMIT) * setting.proportion / 100 - (pos.equity if pos else 0)
        if setting.symbol != 'BTC':
            if (diff > 20 or diff < -60) and abs(diff / setting.instrument.price) > .6:
                logger.info('Recommendation: %s %+.1f (%.2f/%.2f)', setting.symbol,
                            diff / setting.instrument.price, diff, setting.instrument.price)
        elif diff > 10 or diff < -30:
            logger.info('Recommendation: %s %+d', setting.symbol, diff)
        if pos:
            setting.profit_val = round(pos.equity - pos.cost, 2)
            setting.return_pct = pos.total_return_pct
    if positions:
        for pos in positions.values():
            if pos.quantity > 0:
                logger.info('Recommendation: %s %d', pos.symbol, -pos.quantity)

    db.session.commit()
