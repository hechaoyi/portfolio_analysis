import math
import os
from datetime import date, timedelta, datetime

import numpy as np
import pandas_datareader.data as web
from pandas import DataFrame
from scipy import linalg
from scipy.optimize import minimize_scalar
from sortedcontainers import SortedDict

RISK_FREE_RATE_PER_DAY = float(os.environ['RISK_FREE_RATE']) / 252
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
        self.data = self.origin_data[sorted(mask)]

    def drop_mask(self):
        if self.origin_data is not None:
            self.data, self.origin_data = self.origin_data, None

    def moving_average(self):
        return self.data.rolling(self.period).mean().pct_change() * 100

    def statistics(self):
        data = self.moving_average()
        frame = {'len': data.count(), 'mean': data.mean(), 'std': data.std(),
                 'shrp': (data.mean() - RISK_FREE_RATE_PER_DAY) / data.std(),
                 'yield': self.data.T[self.data.index[-1]] / self.data.T[self.data.index[0]] * 100 - 100,
                 'drawdown': self.data.apply(self._max_drawdown)}
        return DataFrame(frame).sort_values('shrp', ascending=False)

    def update_boosts(self, instruments):
        r = self.moving_average()
        boosts = 2 ** ((r.mean() - RISK_FREE_RATE_PER_DAY) / r.std() - .8)
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

        stocks, corr, stat = self.data.columns, self.moving_average().corr(), self.statistics()
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
        data = self.moving_average()
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) * total + (
                C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * round(target, 3)
        m, s = weights.T.dot(mean), math.sqrt(weights.T.dot(cov).dot(weights))
        r = (m - RISK_FREE_RATE_PER_DAY) / s
        return {k: round(v, 3) for k, v in weights.items()}, round(m, 3), round(s, 3), round(r, 3)

    def find_optimal_ratio(self, _lambda=0, bounds=None, total=1):
        assert -2 <= _lambda <= 2

        def attempt(guess):
            if guess > mean.max():
                return float('inf')
            weights = (B * ones.T.dot(cov_inv) - A * mean.T.dot(cov_inv)) / (B * C - A * A) * total + (
                    C * mean.T.dot(cov_inv) - A * ones.T.dot(cov_inv)) / (B * C - A * A) * guess
            m, s = weights.T.dot(mean), math.sqrt(weights.T.dot(cov).dot(weights))
            if m <= 0:
                return float('inf')
            return s / (m ** (1 + _lambda / 5))

        data = self.moving_average()
        mean, cov, ones = data.mean(), data.cov(), np.ones(len(data.columns))
        if not bounds:
            bounds = mean.min(), mean.max()
        elif bounds[0] > mean.max():
            bounds = bounds[0], bounds[0]
        elif bounds[1] < mean.min():
            bounds = bounds[1], bounds[1]
        else:
            bounds = max(mean.min(), bounds[0]), min(mean.max(), bounds[1])
        cov_inv = DataFrame(linalg.pinv(cov.values), cov.columns, cov.index)
        A, B, C = ones.T.dot(cov_inv).dot(mean), mean.T.dot(cov_inv).dot(mean), ones.T.dot(cov_inv).dot(ones)
        res = minimize_scalar(attempt, bounds=bounds, method='Bounded')
        if not res.success:
            print(res)
        return self.optimize(res.x, total)

    def optimize_portfolio(self, min_percent=.1, max_count=3,
                           backlogs_pos_threshold=.9, backlogs_neg_threshold=-.5, _lambda=0, bounds=None):
        candidates, backlogs = set(self.data.columns), []
        corr = self.moving_average().corr()
        while len(candidates) > 1:
            self.setup_mask(candidates)
            ratio, mean, _, shrp = self.find_optimal_ratio(_lambda, bounds)
            coef = round(shrp * (mean ** (_lambda / 5)), 4)
            min_stock = min(ratio, key=lambda s: ratio[s])
            if ratio[min_stock] >= min_percent and len(ratio) <= max_count:
                if backlogs:
                    nxt1, nxt2 = backlogs_pos_threshold + .005, backlogs_neg_threshold - .01
                    if backlogs_pos_threshold >= .99 and len(backlogs) >= 10:
                        nxt1 = backlogs_pos_threshold + .001
                    print(f'retry backlogs {backlogs} at {nxt1:.3f}/{nxt2:.2f} - {shrp}')
                    self.setup_mask([*backlogs, *candidates])
                    sd = self.optimize_portfolio(min_percent, max_count, nxt1, nxt2, _lambda, bounds)
                    if bounds and bounds[0] <= mean <= bounds[1]:
                        sd[(coef, shrp, mean)] = ratio
                        while sd.peekitem(0)[0] < (coef * .9, shrp, mean):
                            sd.popitem(0)
                    return sd
                return SortedDict([((coef, shrp, mean), ratio)])
            candidates.remove(min_stock)
            c1, c2 = corr.loc[min_stock, candidates].max(), corr.loc[min_stock, candidates].min()
            if c1 >= backlogs_pos_threshold or c2 <= backlogs_neg_threshold:
                backlogs.append(min_stock)
            else:
                print(f'evicted {min_stock} {c1:.3f} {c2:.3f}')
        candidate = next(iter(candidates))
        mean, shrp = self._calculate_sharpe_ratio(candidate)
        coef = round(shrp * (mean ** (_lambda / 5)), 4)
        if backlogs:
            nxt1, nxt2 = backlogs_pos_threshold + .005, backlogs_neg_threshold - .01
            if backlogs_pos_threshold >= .99 and len(backlogs) >= 10:
                nxt1 = backlogs_pos_threshold + .001
            print(f'retry backlogs {backlogs} at {nxt1:.3f}/{nxt2:.2f} - {shrp}')
            self.setup_mask([*backlogs, candidate])
            sd = self.optimize_portfolio(min_percent, max_count, nxt1, nxt2, _lambda, bounds)
            if bounds and bounds[0] <= mean <= bounds[1]:
                sd[(coef, shrp, mean)] = {candidate: 1}
                while sd.peekitem(0)[0] < (coef * .9, shrp, mean):
                    sd.popitem(0)
            return sd
        return SortedDict([((coef, shrp, mean), {candidate: 1})])

    def _calculate_sharpe_ratio(self, stock):
        data = self.moving_average()[stock]
        return round(data.mean(), 3), round((data.mean() - RISK_FREE_RATE_PER_DAY) / data.std(), 3)

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
        stat = (data.rolling(self.period).mean().pct_change() * 100).describe().T
        stat['shrp'] = (stat['mean'] - RISK_FREE_RATE_PER_DAY) / stat['std']
        stat['yield'] = data.T[data.index[-1]] / data.T[data.index[0]] * 100 - 100
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
