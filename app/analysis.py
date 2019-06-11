import os
from datetime import date, timedelta
from functools import partial
from math import sqrt

import pandas_datareader.data as web
from numpy import array
from pandas import DataFrame
from scipy.optimize import fsolve, minimize

RISK_FREE_RATE = float(os.environ['RISK_FREE_RATE'])


class Quote:
    def __init__(self, symbols, days_ago):
        start = date.today() - timedelta(days=days_ago)
        self.data = web.DataReader(symbols, 'yahoo', start)['Adj Close']
        self.start = self.data.index[0]
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

    def least_correlated_portfolio(self, period, target, provided=None, *optional, cr=1, dr=1, sr=1):
        def dfs(i, ban):
            if len(buf) == target:
                c = (corr.loc[buf, buf].sum().sum() - target) / 2
                d = stat['drawdown'][buf].sum() / 5
                s = -stat[f'{period}-shrp'][buf].sum()
                score = c * cr + d * dr + s * sr
                if score < best[1]:
                    best[:] = buf[:], score
                    print(buf[:], score, c, d, s)
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

    def optimize(self, period, target):
        data, n = self.data.pct_change(period) * 100, len(self.data.columns)
        mean, cov, w0 = data.mean(), data.cov(), array([1 / n] * n)
        cons = [{'type': 'eq', 'fun': lambda w: sum(w) - 1},
                {'type': 'eq', 'fun': lambda w: w.T.dot(mean) - target}]
        for i in range(n):
            cons.append({'type': 'ineq', 'fun': partial(lambda j, w: w[j], i)})
        res = minimize(lambda w: w.T.dot(cov).dot(w), w0, method='SLSQP', constraints=cons)
        print(res)
        return (dict(zip(self.data.columns, res.x)),
                round(res.x.T.dot(mean), 4), round(sqrt(res.x.T.dot(cov).dot(res.x)), 4))

    def find_optimal_ratio(self, period, init_guess):
        def attempt(guess):
            cons = [{'type': 'eq', 'fun': lambda w: sum(w) - 1},
                    {'type': 'eq', 'fun': lambda w: w.T.dot(mean) - guess}]
            for i in range(n):
                cons.append({'type': 'ineq', 'fun': partial(lambda j, w: w[j], i)})
            x = minimize(lambda w: w.T.dot(cov).dot(w), w0, method='SLSQP', constraints=cons).x
            return x.T.dot(cov).dot(x)

        data, n = self.data.pct_change(period) * 100, len(self.data.columns)
        mean, cov, w0 = data.mean(), data.cov(), array([1 / n] * n)
        return self.optimize(period, fsolve(attempt, init_guess))

    def graph(self, period, portfolio=None):
        data = {col: self.data[col] * (100 / self.data[col][self.start]) for col in self.data.columns}
        if portfolio:
            data['Portfolio'] = sum(data[st] * sh for st, sh in portfolio.items())
            data['Portfolio'] = data['Portfolio'] * (100 / data['Portfolio'][self.start])
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
