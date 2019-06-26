import math
import os
from datetime import date, timedelta, datetime

import numpy as np
import pandas_datareader.data as web
from pandas import DataFrame
from scipy import linalg
from scipy.optimize import fsolve

RISK_FREE_RATE = float(os.environ['RISK_FREE_RATE'])
DATA_READER = {
    'yahoo': lambda symbols, start: web.DataReader(symbols, 'yahoo', start)['Adj Close'],
    'tiingo': lambda symbols, start: web.DataReader(symbols, 'tiingo', start)['adjClose'].unstack('symbol'),
}[os.environ['DATA_READER_VENDOR']]


class Quote:
    def __init__(self, symbols, days_ago):
        start = date.today() - timedelta(days=days_ago)
        self.data = DATA_READER(symbols, start)
        self.start = self.data.index[0]
        self.end = self.data.index[-1]
        self.origin_data = None

    def setup_mask(self, mask):
        if self.origin_data is None:
            self.origin_data = self.data
        self.data = self.origin_data[mask]

    def drop_mask(self):
        if self.origin_data is not None:
            self.data, self.origin_data = self.origin_data, None

    def statistics(self, period, *periods):
        frame = {}
        for p in (period,) + periods:
            r = self.data.pct_change(periods=p) * 100
            frame[f'{p}-len'] = r.count()
            frame[f'{p}-mean'] = r.mean()
            frame[f'{p}-std'] = r.std()
            frame[f'{p}-shrp'] = (r.mean() - RISK_FREE_RATE * p / 252) / r.std()
        frame['drawdown'] = self.data.apply(self._max_drawdown)
        return DataFrame(frame).sort_values(f'{period}-shrp', ascending=False)

    def update_boosts(self, period, instruments):
        r = self.data.pct_change(periods=period) * 100
        boosts = 2 ** ((r.mean() - RISK_FREE_RATE * period / 252) / r.std() - .8)
        for sym, inst in instruments.items():
            inst.boost = round(boosts[sym], 4)
            if inst.is_china():
                inst.boost = round(inst.boost * 1.5, 4)
            if inst.boost is None:
                inst.boost = 1
            inst.boost_last_update = datetime.utcnow()

    def least_correlated_portfolio(self, period, target, provided=None, *optional, cr=1, dr=1, sr=1):
        def dfs(i, ban):
            if buf:
                coef = .2 * (len(buf) - 2)
                if len(buf) == 1:
                    c = .6
                else:
                    c = (corr.loc[buf, buf].sum().sum() - len(buf)) / len(buf) / (len(buf) - 1)
                d = stat['drawdown'][buf].sum() / len(buf) / 5
                s = stat[f'{period}-shrp'][buf].sum() / len(buf)
                score = (c - coef) * cr + (d - coef) * dr - (s - coef) * sr
                if score < best[1]:
                    best[:] = buf[:], score
                    print(buf[:], score, c, d, s)
            if len(buf) == target:
                return
            for j in range(i, len(stocks)):
                if stocks[j] in buf or stocks[j] == ban:
                    continue
                buf.append(stocks[j])
                dfs(j + 1, ban)
                buf.pop()

        stocks, corr, stat = self.data.columns, self.data.pct_change(period).corr(), self.statistics(period)
        buf = provided if provided else []
        best = [None, float('inf')]
        dfs(0, None)
        if optional:
            for o in optional:
                b = buf.pop(o)
                dfs(0, b)
                buf.insert(o, b)
        return best[0]

    def optimize(self, period, target):  # minimum=defaultdict(float)
        data = self.data.pct_change(period) * 100
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) + (
                C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * target
        return weights, round(weights.T.dot(mean), 4), round(math.sqrt(weights.T.dot(cov).dot(weights)), 4)
        # data, n = self.data.pct_change(period) * 100, len(self.data.columns)
        # mean, cov, w0 = data.mean(), data.cov(), array([1 / n] * n)
        # cons = [{'type': 'eq', 'fun': lambda w: sum(w) - 1},
        #         {'type': 'eq', 'fun': lambda w: w.T.dot(mean) - target}]
        # for i in range(n):
        #     cons.append({'type': 'ineq', 'fun': partial(lambda j, w: w[j] - minimum[j], i)})
        # res = minimize(lambda w: w.T.dot(cov).dot(w), w0, method='SLSQP', constraints=cons)
        # print(res)
        # return (dict(zip(self.data.columns, (round(x, 2) for x in res.x))),
        #         round(res.x.T.dot(mean), 4), round(sqrt(res.x.T.dot(cov).dot(res.x)), 4))

    def find_optimal_ratio(self, period, init_guess):
        def attempt(guess):
            weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) + (
                    C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * guess
            return weights.T.dot(cov).dot(weights)
            # cons = [{'type': 'eq', 'fun': lambda w: sum(w) - 1},
            #         {'type': 'eq', 'fun': lambda w: w.T.dot(mean) - guess}]
            # for i in range(n):
            #     cons.append({'type': 'ineq', 'fun': partial(lambda j, w: w[j] - minimum[j], i)})
            # x = minimize(lambda w: w.T.dot(cov).dot(w), w0, method='SLSQP', constraints=cons).x
            # return x.T.dot(cov).dot(x)

        data = self.data.pct_change(period) * 100
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        # data, n = self.data.pct_change(period) * 100, len(self.data.columns)
        # mean, cov, w0 = data.mean(), data.cov(), array([1 / n] * n)
        return self.optimize(period, fsolve(attempt, init_guess))

    def graph(self, period, portfolio=None, drop_components=False):
        data = {col: self.data[col] * (100 / self.data[col][self.start]) for col in self.data.columns}
        if portfolio:
            data['Portfolio'] = sum(data[st] * sh for st, sh in portfolio.items())
            data['Portfolio'] = data['Portfolio'] * (100 / data['Portfolio'][self.start])
            if drop_components:
                for st in portfolio:
                    del data[st]
        data = DataFrame(data)
        data.plot(figsize=(12, 8), grid=1)
        stat = (data.pct_change(period) * 100).describe().T
        stat['shrp'] = (stat['mean'] - RISK_FREE_RATE * period / 252) / stat['std']
        stat['drawdown'] = data.apply(self._max_drawdown)
        return stat.sort_values('shrp', ascending=False)

    @staticmethod
    def _max_drawdown(series):
        max_price_so_far, max_drawdown_so_far = float('-inf'), 0
        result = None
        for p in series:
            drawdown = max_price_so_far - p
            if drawdown > max_drawdown_so_far:
                max_drawdown_so_far = drawdown
                result = max_drawdown_so_far / max_price_so_far * 100
            max_price_so_far = max(max_price_so_far, p)
        return result

    @staticmethod
    def usd_cny():
        print(web.DataReader('USD/CNY', 'av-forex')['USD/CNY']['Exchange Rate'])
