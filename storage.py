import os
import datetime
import pathlib

import msgpack
import numpy as np
import pandas as pd
from sortedcontainers import SortedListWithKey
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from db.tables import Base


def dt_to_micro_timestamp(dt):
    return int(dt.timestamp() * 1E6)


def micros_timestamp_to_dt(ts_micros):
    return datetime.datetime.fromtimestamp(ts_micros / 1E6)


def numpy_fillna(data):
    # Get lengths of each row of data
    lens = np.array([len(i) for i in data])

    # Mask of valid places in each row
    mask = np.arange(lens.max()) < lens[:, None]

    # Setup output array and put elements from data into masked positions
    out = np.zeros(mask.shape, dtype=data.dtype)
    out[mask] = np.concatenate(data)
    return out.tolist()


class MemoryCache:
    # TODO: background delay before forcing flush
    def __init__(self, cache_size, time_margin, callback_when_full):
        self.cache_size = cache_size
        self.time_margin = time_margin
        self.callback_when_full = callback_when_full

        self.dump = False

        self.cache = {}
        self.cache_nov = 0

        self.next_cache = {}
        self.next_cache_nov = 0

    def add_data(self, timestamp, datatype, data, number_of_values):
        if datatype not in self.cache:
            self.cache[datatype] = SortedListWithKey(key=lambda x: x[0])
        if datatype not in self.next_cache:
            self.next_cache[datatype] = SortedListWithKey(key=lambda x: x[0])

        if self.dump:
            if timestamp < dt_to_micro_timestamp(datetime.datetime.now() - self.time_margin):
                self.cache[datatype].add((timestamp, data))
                self.cache_nov += number_of_values
            else:
                self.next_cache[datatype].add((timestamp, data))
                self.next_cache_nov += number_of_values
        else:
            self.cache[datatype].add((timestamp, data))
            self.cache_nov += number_of_values

        if not self.dump and self.cache_nov >= self.cache_size:
            self.dump = True

        if self.dump:
            do = True
            for k, _ in self.cache.items():
                if len(self.cache[k]) > 0 and self.cache[k][-1][0] >= dt_to_micro_timestamp(datetime.datetime.now() - self.time_margin):
                    do = False
                    break
            if do:
                # TODO: Background-ize
                self.callback_when_full(self.cache)

                self.cache = self.next_cache
                self.cache_nov = self.next_cache_nov
                self.next_cache = {}
                self.next_cache_nov = 0

                self.dump = False


class ImmutableStore:
    def __init__(self, location: str, cache_size: int = 1E10, time_margin=datetime.timedelta(minutes=5), partitioning_depth=4):
        """
        cache_size: the number of values needed to dump the cache to disk
        """
        self.location = location
        self.partitioning = '%Y/%m/%d/%H/%M/%S'.split('/')[:partitioning_depth]
        self.cache = MemoryCache(cache_size=cache_size, time_margin=time_margin,
                                 callback_when_full=self._write_block_callback)
        # self.avrorwer = AvroRWer(schema=avro_schema)
        self.datatypes = {
            'lf': ['source_id', 'type_id', 'value', 'timestamp_micros'],
            'hf': ['source_id', 'type_id', 'values', 'start_micros', 'end_micros', 'frequency']
        }

    def _write_block_callback(self, full_cache: dict):
        date_start = None
        date_end = None

        json_store = {
            'lf': {
                'source_id': [],
                'type_id': [],
                'timestamp_micros': [],
                'value': [],
            },
            'hf': {
                'source_id': [],
                'type_id': [],
                'start_micros': [],
                'end_micros': [],
                'frequency': [],
                'values': [],
            }
        }

        for dtt, sorted_list in full_cache.items():
            if len(sorted_list) == 0:
                continue
            if date_start is None or date_start > sorted_list[0][0]:
                date_start = sorted_list[0][0]
            if date_end is None or date_end < sorted_list[-1][0]:
                date_end = sorted_list[-1][0]

            for (timestamp, data) in sorted_list:
                for sub_dtt in self.datatypes[dtt]:
                    json_store[dtt][sub_dtt].append(data[sub_dtt])

        dt_date_first = datetime.datetime.fromtimestamp(date_start / 1E6)

        dst_dir = os.path.join(self.location, dt_date_first.strftime('/'.join(self.partitioning)))
        pathlib.Path(dst_dir).mkdir(parents=True, exist_ok=True)

        dst = os.path.join(dst_dir, '{}-{}.msgpck'.format(date_start, date_end))

        with open(dst, 'wb') as f:
            msgpack.pack(json_store, f)

    def write_lf(self, source_id: int, type_id: int, timestamp_micros: int, value: float):
        self.cache.add_data(timestamp_micros, 'lf',
                            {'source_id': source_id, 'type_id': type_id, 'timestamp_micros': timestamp_micros,
                             'value': value},
                            number_of_values=1)

    def write_hf(self, source_id: int, type_id: int, start_micros: int, frequency: float, values: list):
        end_micros = start_micros + int((len(values) / frequency) * 1E6)
        self.cache.add_data(start_micros, 'hf',
                            {'source_id': source_id, 'type_id': type_id, 'start_micros': start_micros,
                             'end_micros': end_micros,
                             'frequency': frequency, 'values': values},
                            number_of_values=len(values))

    def _find_blocks(self, start_micros, end_micros):
        dt_start = micros_timestamp_to_dt(start_micros)
        dt_end = micros_timestamp_to_dt(end_micros)

        ls = []
        le = []

        m = {'Y': 'year', 'm': 'month', 'd': 'day', 'H': 'hour', 'M': 'minute', 'S': 'second'}
        for e in self.partitioning:
            ls.append(str(getattr(dt_start, m[e[1]])))
            le.append(str(getattr(dt_end, m[e[1]])))

        def rec_explore(dir, previous=None, depth=0):
            if previous is None:
                previous = []

            if depth == len(self.partitioning):
                res = []
                for f in os.listdir(os.path.join(dir, *previous)):
                    s, e = [int(k) for k in f.split('.')[0].split('-')]
                    if e < start_micros or s > end_micros:
                        continue
                    res.append(os.path.join(dir, *previous, f))
                return res
            else:
                result = []
                for p in os.listdir(os.path.join(dir, *previous)):
                    curr = int(''.join(previous) + p)
                    if int(dt_start.strftime(''.join(self.partitioning[:depth + 1]))) <= curr <= int(
                            dt_end.strftime(''.join(self.partitioning[:depth + 1]))):
                        result += rec_explore(dir, previous + [p], depth + 1)
                return result

        return sorted(rec_explore(self.location))

    def read_blocks(self, start_micros, end_micros, lf=True, hf=True, **filters):
        if not lf and not hf:
            yield from []
            return

        t0 = datetime.datetime.now()

        blocks = self._find_blocks(start_micros, end_micros)

        def _subfun(json_store, k):
            if k not in ['lf', 'hf']:
                raise NotImplementedError()

            if k == 'lf':
                df = pd.DataFrame.from_dict(json_store['lf']).astype({'source_id': int,
                                                              'type_id': int,
                                                              'timestamp_micros': int,
                                                              'value': float})
                where = 'timestamp_micros >= {} & timestamp_micros < {}'.format(start_micros, end_micros)
            elif k == 'hf':
                df = pd.DataFrame.from_dict(json_store['hf']).astype({'source_id': int,
                                                              'type_id': int,
                                                              'start_micros': int,
                                                              'end_micros': int,
                                                              'frequency': float,
                                                              'values': list})
                where = 'start_micros >= {} & end_micros < {}'.format(start_micros, end_micros)
            else:
                raise NotImplementedError()

            for fn, fv in filters.items():
                where += ' & {}=={}'.format(fn, fv)

            df = df.query(where)

            if k == 'lf':
                df = df.sort_values('timestamp_micros')
            elif k == 'hf':
                df = df.sort_values('start_micros')
            dff = df.transpose().to_dict('split')
            result = {}
            for m, col in enumerate(dff['index']):
                result[col] = dff['data'][m]
            return result

        for block in blocks:
            res = {}
            t1 = datetime.datetime.now()
            with open(block, 'rb') as b:
                print(block)
                json_store = msgpack.unpack(b, raw=False)
                # json_store = pickle.load(b)
            print('msgpack.unpack took {}'.format(datetime.datetime.now()-t1))
            if lf:
                res['lf'] = _subfun(json_store, 'lf')
            if hf:
                res['hf'] = _subfun(json_store, 'hf')
            print('yield took {}'.format(datetime.datetime.now()-t0))
            yield res

    def read_all_blocks(self, start_micros, end_micros, lf=True, hf=True, **filters):
        dd = {
            'lf': {
                'source_id': [],
                'type_id': [],
                'timestamp_micros': [],
                'value': [],
            },
            'hf': {
                'source_id': [],
                'type_id': [],
                'start_micros': [],
                'end_micros': [],
                'frequency': [],
                'values': [],
            }
        }
        for d in self.read_blocks(start_micros, end_micros, lf=lf, hf=hf, **filters):
            ff = []
            if lf:
                ff.append('lf')
            if hf:
                ff.append('hf')
            for k in ff:
                for kk, vv in d[k].items():
                    dd[k][kk] += vv
        return dd


class MutableStore:
    def __init__(self, url="sqlite:///pancarte.sqlite3"):
        self._engine = create_engine(url)
        self._session_class = sessionmaker(bind=self._engine)
        self.get_session = self._session_class()

        Base.metadata.create_all(self._engine)

    def create(self, model, **kwargs):
        o = model(**kwargs)

        session = self._session_class()
        try:
            session.add(o)
            session.commit()
            id = o.id
        except IntegrityError:
            session.rollback()
            raise
        finally:
            session.close()

        return self.get(model, id=id)

    def get(self, model, **kwargs):
        return self.get_session.query(model).filter_by(**kwargs).first()

    def get_all(self, model, **kwargs):
        return self.get_session.query(model).filter_by(**kwargs).all()

    def update(self, model, id, **kwargs):
        session = self._session_class()

        try:
            session.query(model).filter(model.id == id).update(kwargs)
            session.commit()
        except IntegrityError:
            session.rollback()
            raise
        finally:
            session.close()

        return self.get(model, id=id)

    def delete(self, model, id):
        o = model(id=id)

        session = self._session_class()
        try:
            session.add(o)
            session.commit()
        except IntegrityError:
            session.rollback()
            raise
        finally:
            session.close()

        return self.get(model, id=id)
