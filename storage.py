import os
import datetime
import pathlib

from sortedcontainers import SortedListWithKey

import avro.schema
import avro.io
import avro.datafile
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, scoped_session

from db.tables import Base


def dt_to_nano_timestamp(dt):
    return int(dt.timestamp() * 1E9)


class AvroRWer:
    def __init__(self, schema: str):
        self.schema = avro.schema.Parse(open(schema, "r").read())
        self.datum_writer = avro.io.DatumWriter(self.schema)
        self.datum_reader = avro.io.DatumReader(self.schema)

    def write(self, dst: str, data: dict):
        df_writer = avro.datafile.DataFileWriter(writer=open(dst, 'wb'),
                                                 datum_writer=self.datum_writer,
                                                 writer_schema=self.schema,
                                                 codec='null')
        df_writer.append(data)
        df_writer.close()

    def read(self, src):
        df_reader = avro.datafile.DataFileReader(reader=open(src, "rb"),
                                                 datum_reader=self.datum_reader)
        for record in df_reader:
            return record


class MemoryCache:
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
            if timestamp < dt_to_nano_timestamp(datetime.datetime.now() - self.time_margin):
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
                if self.cache[k][-1][0] >= dt_to_nano_timestamp(datetime.datetime.now() - self.time_margin):
                    do = False
                    break
            if do:
                self.callback_when_full(self.cache)

                self.cache = self.next_cache
                self.cache_nov = self.next_cache_nov
                self.next_cache = {}
                self.next_cache_nov = 0

                self.dump = False


class ImmutableStore:
    def __init__(self, location: str, avro_schema: str, cache_size: int = 1E10,
                 time_margin=datetime.timedelta(minutes=5)):
        """
        cache_size: the number of values needed to dump the cache to disk
        """
        self.location = location
        self.cache = MemoryCache(cache_size=cache_size, time_margin=time_margin,
                                 callback_when_full=self._write_block_callback)
        self.avrorwer = AvroRWer(schema=avro_schema)
        self.datatypes = {
            'lf': ['source_id', 'type_id', 'value', 'timestamp'],
            'hf': ['source_id', 'type_id', 'values', 'start_date', 'frequency']
        }

    def _write_block_callback(self, full_cache: dict):
        date_first = None
        date_end = None

        df = {
            'lf': {
                'source_id': [],
                'type_id': [],
                'timestamp': [],
                'value': [],
            },
            'hf': {
                'source_id': [],
                'type_id': [],
                'start_date': [],
                'frequency': [],
                'values': [],
            }
        }

        for dtt, sorted_list in full_cache.items():
            if date_first is None or date_first > sorted_list[0][0]:
                date_first = sorted_list[0][0]
            if date_end is None or date_end < sorted_list[-1][0]:
                date_end = sorted_list[-1][0]

            for (timestamp, data) in sorted_list:
                for sub_dtt in self.datatypes[dtt]:
                    ddd = data[sub_dtt]
                    if type(ddd) == list:
                        df[dtt][sub_dtt].append({'v': ddd})
                    else:
                        df[dtt][sub_dtt].append(ddd)

        df['start_date'] = int(date_first / 1E3)

        dt_date_first = datetime.datetime.fromtimestamp(date_first / 1E9)

        dst_dir = os.path.join(self.location, dt_date_first.strftime('%Y/%m/%d/%H'))
        pathlib.Path(dst_dir).mkdir(parents=True, exist_ok=True)

        dst = os.path.join(dst_dir, '{}-{}.v1.avro'.format(date_first, date_end))

        self.avrorwer.write(dst=dst, data=df)

    def write_lf(self, source_id: int, type_id: int, timestamp: int, value: float):
        self.cache.add_data(timestamp, 'lf',
                            {'source_id': source_id, 'type_id': type_id, 'timestamp': timestamp, 'value': value},
                            number_of_values=1)

    def write_hf(self, source_id: int, type_id: int, start_date: int, frequency: float, values: list):
        self.cache.add_data(start_date, 'hf',
                            {'source_id': source_id, 'type_id': type_id, 'start_date': start_date,
                             'frequency': frequency, 'values': values},
                            number_of_values=len(values))


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

    def get_all(self, model, **kwargs):
        return self.get_session.query(model).filter_by(**kwargs).all()
