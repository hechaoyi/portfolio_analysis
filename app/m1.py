import os
from datetime import datetime

import requests

from . import db

M1_USERNAME = os.environ['M1_USERNAME']
M1_PASSWORD = os.environ['M1_PASSWORD']
M1_ACCT_ID = os.environ['M1_ACCT_ID']


class M1Portfolio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    value = db.Column(db.Float)
    day_net_cash_flow = db.Column(db.Float)
    day_capital_gain = db.Column(db.Float)
    day_dividend_gain = db.Column(db.Float)
    day_total_gain = db.Column(db.Float)
    day_return_rate = db.Column(db.Float)
    day_start_time = db.Column(db.DateTime)
    day_start_value = db.Column(db.Float)
    all_net_cash_flow = db.Column(db.Float)
    all_capital_gain = db.Column(db.Float)
    all_dividend_gain = db.Column(db.Float)
    all_total_gain = db.Column(db.Float)
    all_return_rate = db.Column(db.Float)
    all_start_time = db.Column(db.DateTime)
    all_start_value = db.Column(db.Float)
    last_update = db.Column(db.DateTime)

    def __str__(self):
        return f'[{self.date}] {self.value} | ' \
            f'{self.day_total_gain}/{self.day_return_rate}% | {self.all_total_gain}/{self.all_return_rate}%'

    @classmethod
    def create_or_update(cls):
        token = graphql(f'mutation {{ authenticate(input: {{username: "{M1_USERNAME}", password: "{M1_PASSWORD}"}})'
                        f' {{ accessToken }}}}')['data']['authenticate']['accessToken']
        day = graphql(f'{{ node(id: "{M1_ACCT_ID}") {{ ... on PortfolioSlice {{ performance(period: ONE_DAY) '
                      f'{{ startValue {{ date, value }}, endValue {{ date, value }}, '
                      f'moneyWeightedRateOfReturn, totalGain, capitalGain, earnedDividends, netCashFlow '
                      f'}}}}}}}}', {'Authorization': f'Bearer {token}'})['data']['node']['performance']
        if day['endValue']['date'][-13:] == '00:00:00.000Z':
            return None
        _date = parse_datetime(day['endValue']['date']).date()
        inst = cls.query.filter_by(date=_date).first()
        if not inst:
            inst = cls(date=_date)
            db.session.add(inst)
        inst.value = day['endValue']['value']

        inst.day_net_cash_flow = day['netCashFlow']
        inst.day_capital_gain = day['capitalGain']
        inst.day_dividend_gain = day['earnedDividends']
        inst.day_total_gain = day['totalGain']
        inst.day_return_rate = day['moneyWeightedRateOfReturn']
        inst.day_start_time = parse_datetime(day['startValue']['date'])
        inst.day_start_value = day['startValue']['value']

        all = graphql(f'{{ node(id: "{M1_ACCT_ID}") {{ ... on PortfolioSlice {{ performance(period: MAX) '
                      f'{{ startValue {{ date, value }}, '
                      f'moneyWeightedRateOfReturn, totalGain, capitalGain, earnedDividends, netCashFlow '
                      f'}}}}}}}}', {'Authorization': f'Bearer {token}'})['data']['node']['performance']
        inst.all_net_cash_flow = all['netCashFlow']
        inst.all_capital_gain = all['capitalGain']
        inst.all_dividend_gain = all['earnedDividends']
        inst.all_total_gain = all['totalGain']
        inst.all_return_rate = all['moneyWeightedRateOfReturn']
        inst.all_start_time = parse_datetime(all['startValue']['date'])
        inst.all_start_value = all['startValue']['value']

        inst.last_update = datetime.utcnow()
        return inst


def graphql(query, headers=None):
    return requests.post('https://lens.m1finance.com/graphql', json={'query': query}, headers=headers).json()


def parse_datetime(dt):
    return datetime.fromisoformat(dt.replace('Z', '+00:00'))


def update_m1_account():
    logger = db.get_app().logger
    logger.info('%s', M1Portfolio.create_or_update())
    db.session.commit()
