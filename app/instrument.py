from datetime import datetime

from . import db


class Instrument(db.Model):
    symbol = db.Column(db.String(8), primary_key=True)
    robinhood_id = db.Column(db.String(40), unique=True, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    list_date = db.Column(db.Date, nullable=False)
    popularity = db.Column(db.Integer, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False)
    # fundamentals
    description = db.Column(db.String(2048))
    sector = db.Column(db.String(40))
    industry = db.Column(db.String(40))
    market_cap = db.Column(db.Integer)
    low_52_weeks = db.Column(db.Float)
    high_52_weeks = db.Column(db.Float)
    average_volume = db.Column(db.Integer)
    pe_ratio = db.Column(db.Float)
    pb_ratio = db.Column(db.Float)
    dividend_yield = db.Column(db.Float)
    ceo = db.Column(db.String(40))
    headquarters_city = db.Column(db.String(40))
    headquarters_state = db.Column(db.String(40))
    num_employees = db.Column(db.Integer)
    year_founded = db.Column(db.Integer)
    # relationship
    tags = db.relationship('Tag')

    def __str__(self):
        return f'[{self.symbol}] {self.name} ({self.sector}) {self.popularity}'

    @classmethod
    def create_or_update(cls, rid, popularity=None, recommended=None):
        rh = db.get_app().robinhood
        json = rh.get(f'https://api.robinhood.com/instruments/{rid}/').json()
        if not json.get('tradeable') or not json.get('list_date'):
            return None

        symbol = json['symbol']
        inst = cls.query.get(symbol)
        if not inst:
            inst = cls(symbol=symbol)
            db.session.add(inst)

        inst.robinhood_id = json['id']
        inst.name = json['simple_name'] or json['name']
        inst.list_date = datetime.strptime(json['list_date'], '%Y-%m-%d')
        inst.last_update = datetime.utcnow()

        if popularity is not None:
            inst.popularity = popularity
        else:
            json = rh.get(f'https://api.robinhood.com/instruments/popularity/?ids={rid}').json()
            inst.popularity = int(json['results'][0]['num_open_positions'])

        json = rh.get(f'https://api.robinhood.com/fundamentals/{rid}/').json()
        inst.description = json['description'] if json['description'] else None
        inst.sector = json['sector'] if json['sector'] else None
        inst.industry = json['industry'] if json['industry'] else None
        inst.market_cap = int(float(json['market_cap'])) if json['market_cap'] else None
        inst.low_52_weeks = float(json['low_52_weeks']) if json['low_52_weeks'] else None
        inst.high_52_weeks = float(json['high_52_weeks']) if json['high_52_weeks'] else None
        inst.average_volume = int(float(json['average_volume'])) if json['average_volume'] else None
        inst.pe_ratio = float(json['pe_ratio']) if json['pe_ratio'] else None
        inst.pb_ratio = float(json['pb_ratio']) if json['pb_ratio'] else None
        inst.dividend_yield = float(json['dividend_yield']) if json['dividend_yield'] else None
        inst.ceo = json['ceo'] if json['ceo'] else None
        inst.headquarters_city = json['headquarters_city'] if json['headquarters_city'] else None
        inst.headquarters_state = json['headquarters_state'] if json['headquarters_state'] else None
        inst.num_employees = int(json['num_employees']) if json['num_employees'] else None
        inst.year_founded = int(json['year_founded']) if json['year_founded'] else None

        json = rh.get(f'https://api.robinhood.com/midlands/tags/instrument/{rid}/').json()
        for tag in json['tags']:
            name = tag['name']
            if not any(t.name == name for t in inst.tags):
                inst.tags.append(Tag(symbol=symbol, name=name))
            if recommended is not None:
                recommended.extend(url[len('https://api.robinhood.com/instruments/'):-1]
                                   for url in tag['instruments'][:20])

        if recommended is not None:
            json = rh.get(f'https://dora.robinhood.com/instruments/similar/{rid}/').json()
            recommended.extend(s['instrument_id'] for s in json['similar'])
        return inst


class Tag(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(8), db.ForeignKey('instrument.symbol'), nullable=False)
    name = db.Column(db.String(40), nullable=False)


def update_instruments(popularity_cutoff=300, always=False):
    import collections
    rh, logger = db.get_app().robinhood, db.get_app().logger
    queue, seen, count = collections.deque(), set(), 0
    for url in (
            'https://api.robinhood.com/midlands/tags/tag/100-most-popular/',
            'https://api.robinhood.com/midlands/tags/tag/etf/',
            'https://api.robinhood.com/midlands/tags/tag/reit/',
            'https://api.robinhood.com/midlands/tags/tag/china/',):
        queue.extend(url[len('https://api.robinhood.com/instruments/'):-1]
                     for url in rh.get(url).json()['instruments'][:100])
    while queue:
        chunk = []
        while queue and len(chunk) < 50:
            s = queue.popleft()
            if s not in seen:
                seen.add(s)
                chunk.append(s)
        if not chunk:
            continue
        json = rh.get('https://api.robinhood.com/instruments/popularity/', params={'ids': ','.join(chunk)}).json()
        chunk = {pop['instrument'][len('https://api.robinhood.com/instruments/'):-1]: pop['num_open_positions']
                 for pop in json['results'] if pop['num_open_positions'] >= popularity_cutoff}
        if not chunk:
            continue
        for s, p in chunk.items():
            try:
                if always or not Instrument.query.filter_by(robinhood_id=s).first():
                    logger.info('%d. %s', count + 1, Instrument.create_or_update(s, p, queue))
                    count += 1
                else:
                    json = rh.get(f'https://api.robinhood.com/midlands/tags/instrument/{s}/').json()
                    queue.extend(url[len('https://api.robinhood.com/instruments/'):-1]
                                 for tag in json['tags'] for url in tag['instruments'][:20])
                    json = rh.get(f'https://dora.robinhood.com/instruments/similar/{s}/').json()
                    queue.extend(s['instrument_id'] for s in json['similar'])
            except:
                logger.warn('%s', s, exc_info=1)
        db.session.commit()
