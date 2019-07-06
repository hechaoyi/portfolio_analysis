import math
import os
from datetime import date, timedelta, datetime

import numpy as np
import pandas_datareader.data as web
from pandas import DataFrame
from scipy import linalg
from scipy.optimize import minimize_scalar

RISK_FREE_RATE = float(os.environ['RISK_FREE_RATE'])
DATA_READER = {
    'yahoo': lambda symbols, start: web.DataReader(symbols, 'yahoo', start)['Adj Close'],
    'tiingo': lambda symbols, start: web.DataReader(symbols, 'tiingo', start)['adjClose'].unstack('symbol'),
}[os.environ['DATA_READER_VENDOR']]


class Quote:
    def __init__(self, symbols, days_ago, period):
        start = date.today() - timedelta(days=days_ago)
        self.data = DATA_READER(symbols, start)
        self.period = period
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

    def statistics(self):
        data = self.data.pct_change(periods=self.period) * 100
        frame = {'len': data.count(), 'mean': data.mean(), 'std': data.std(),
                 'shrp': (data.mean() - RISK_FREE_RATE * self.period / 252) / data.std(),
                 'drawdown': self.data.apply(self._max_drawdown)}
        return DataFrame(frame).sort_values('shrp', ascending=False)

    def update_boosts(self, instruments):
        r = self.data.pct_change(periods=self.period) * 100
        boosts = 2 ** ((r.mean() - RISK_FREE_RATE * self.period / 252) / r.std() - .8)
        for sym, inst in instruments.items():
            inst.boost = round(boosts[sym], 4)
            if inst.is_china():
                inst.boost = round(inst.boost * 1.5, 4)
            if inst.boost is None:
                inst.boost = 1
            inst.boost_last_update = datetime.utcnow()

    def least_correlated_portfolio(self, target, provided=None, *optional, cr=1, dr=1, sr=1):
        def dfs(i, ban):
            if buf:
                coef = .1 * (len(buf) - 1)
                if len(buf) == 1:
                    c = .8
                else:
                    c = (corr.loc[buf, buf].sum().sum() - len(buf)) / len(buf) / (len(buf) - 1)
                d = stat['drawdown'][buf].sum() / len(buf) / 5
                s = stat['shrp'][buf].sum() / len(buf)
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

        stocks, corr, stat = self.data.columns, self.data.pct_change(self.period).corr(), self.statistics()
        buf = provided if provided else []
        best = [None, float('inf')]
        dfs(0, None)
        if optional:
            for o in optional:
                b = buf.pop(o)
                dfs(0, b)
                buf.insert(o, b)
        return best[0]

    def optimize(self, target, total=1):
        data = self.data.pct_change(self.period) * 100
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) * total + (
                C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * target
        m, s = weights.T.dot(mean), math.sqrt(weights.T.dot(cov).dot(weights))
        r = (m - RISK_FREE_RATE * self.period / 252) / s
        return {k: round(v, 2) for k, v in weights.items()}, round(m, 4), round(s, 4), round(r, 4)

    def find_optimal_ratio(self, total=1, lmd=0):
        def attempt(guess):
            weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) * total + (
                    C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * guess
            m, s = weights.T.dot(mean), math.sqrt(weights.T.dot(cov).dot(weights))
            return (RISK_FREE_RATE * self.period / 252 - m) / s + lmd * s

        data = self.data.pct_change(self.period) * 100
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        return self.optimize(minimize_scalar(attempt, bounds=(min(mean), max(mean))).x, total)

    def optimize_portfolio(self, candidates, min_percent=.008, min_count=0, total=1):
        candidates, portfolio = set(candidates), {}
        corr = self.data.pct_change(self.period).corr()
        while len(candidates) > 1:
            self.setup_mask(candidates)
            ratio, *_ = self.find_optimal_ratio(total - len(portfolio) * min_percent)
            min_stock = min(ratio, key=lambda s: ratio[s])
            if ratio[min_stock] > min_percent:
                portfolio.update(ratio)
                return portfolio
            candidates.remove(min_stock)
            portfolio[min_stock] = min_percent
            if len(portfolio) > min_count:
                max_stock = max(portfolio, key=lambda s: corr.loc[s, (portfolio.keys() - {s}) | candidates].max())
                portfolio.pop(max_stock)
                c = corr.loc[max_stock, (portfolio.keys() - {max_stock}) | candidates].max()
                print(f'evicted {max_stock} {c}')
        if candidates:
            portfolio[candidates.pop()] = total - len(portfolio) * min_percent
        return portfolio

    def graph(self, portfolio=None, drop_components=False):
        data = {col: self.data[col] * (100 / self.data[col][self.start]) for col in self.data.columns}
        if portfolio:
            data['Portfolio'] = sum(data[st] * sh for st, sh in portfolio.items())
            data['Portfolio'] = data['Portfolio'] * (100 / data['Portfolio'][self.start])
            if drop_components:
                for st in portfolio:
                    del data[st]
        data = DataFrame(data)
        data.plot(figsize=(12, 8), grid=1)
        stat = (data.pct_change(self.period) * 100).describe().T
        stat['shrp'] = (stat['mean'] - RISK_FREE_RATE * self.period / 252) / stat['std']
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
