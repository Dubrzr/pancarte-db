from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, BigInteger, ForeignKey, Float

Base = declarative_base()


class AnnotationType(Base):
    __tablename__ = 'annotation_type'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class TimestampAnnotation(Base):
    __tablename__ = 'timestamp_annotation'

    id = Column(Integer, primary_key=True)
    source_id = Column(BigInteger, nullable=False)
    type_id = Column(BigInteger, ForeignKey("annotation_type.id"), nullable=False)
    comment = Column(String)

    timestamp = Column(BigInteger, nullable=False)

    def __repr__(self):
        return "{}(id={}, source_id={}, type_id={}, timestamp={}, comment={})"\
            .format(self.__tablename__, self.id, self.source_id, self.type_id, self.timestamp, self.comment)


class TimerangeAnnotation(Base):
    __tablename__ = 'timestamp_annotation'

    id = Column(Integer, primary_key=True)
    source_id = Column(BigInteger, nullable=False)
    type_id = Column(BigInteger, ForeignKey("annotation_type.id"), nullable=False)
    value = Column(Float)
    comment = Column(String)

    start = Column(BigInteger, nullable=False)
    end = Column(BigInteger, nullable=False)

    def __repr__(self):
        return "{}(id={}, source_id={}, type_id={}, start={}, end={} comment={})"\
            .format(self.__tablename__, self.id, self.source_id, self.type_id, self.start, self.end, self.comment)

