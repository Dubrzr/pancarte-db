from sqlalchemy import Column, Integer, String, BigInteger, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AnnotationType(Base):
    __tablename__ = 'annotation_type'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
        }


class TimestampAnnotation(Base):
    __tablename__ = 'timestamp_annotation'

    id = Column(Integer, primary_key=True)
    source_id = Column(BigInteger, nullable=False)
    type_id = Column(BigInteger, ForeignKey("annotation_type.id"), nullable=False)
    value = Column(Float, default=0.)
    comment = Column(String, default='')

    timestamp_micros = Column(BigInteger, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'source_id': self.source_id,
            'type_id': self.type_id,
            'value': self.value,
            'comment': self.comment,
            'timestamp_micros': self.timestamp_micros,
        }

    def __repr__(self):
        return "{}(id={}, source_id={}, type_id={}, timestamp={}, comment={})"\
            .format(self.__tablename__, self.id, self.source_id, self.type_id, self.timestamp_micros, self.comment)


class TimerangeAnnotation(Base):
    __tablename__ = 'timerange_annotation'

    id = Column(Integer, primary_key=True)
    source_id = Column(BigInteger, nullable=False)
    type_id = Column(BigInteger, ForeignKey("annotation_type.id"), nullable=False)
    value = Column(Float, default=0.)
    comment = Column(String, default='')

    start_micros = Column(BigInteger, nullable=False)
    end_micros = Column(BigInteger, nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'source_id': self.source_id,
            'type_id': self.type_id,
            'value': self.value,
            'comment': self.comment,
            'start_micros': self.start_micros,
            'end_micros': self.end_micros,
        }

    def __repr__(self):
        return "{}(id={}, source_id={}, type_id={}, start={}, end={} comment={})"\
            .format(self.__tablename__, self.id, self.source_id, self.type_id, self.start_micros, self.end_micros, self.comment)
