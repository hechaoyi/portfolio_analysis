from datetime import datetime, date

from sqlalchemy import func

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
    return_pct = db.Column(db.Float, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False)
    # relationship
    previous_id = db.Column(db.Integer, db.ForeignKey('portfolio.id'))
    previous = db.relationship('Portfolio', remote_side=[id], backref='next')

    def __str__(self):
        return f'[{self.date}] {self.equity} {self.return_pct}%'

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
        prev_equity, prev_cost = (inst.previous.equity, inst.previous.cost) if inst.previous else (0, 0)
        inst.return_pct = round(inst.equity / (prev_equity + cost - prev_cost) * 100 - 100, 2)
        inst.last_update = datetime.utcnow()
        return inst


def update_account():
    rh, logger = db.get_app().robinhood, db.get_app().logger
    for trans in reversed(rh.get('https://api.robinhood.com/ach/transfers/').json()['results']):
        Transfer.create_or_update(trans['id'], trans['created_at'], trans['direction'], trans['amount'])
    cost = db.session.query(func.sum(Transfer.amount)).scalar()
    logger.info('%s', Portfolio.create_or_update(cost))
    db.session.commit()
