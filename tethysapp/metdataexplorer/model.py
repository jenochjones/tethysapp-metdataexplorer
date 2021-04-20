from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer

Base = declarative_base()


class Thredds(Base):
    __tablename__ = 'thredds'

    id = Column(Integer, primary_key=True)
    type = Column(String(100))
    group = Column(String(100))
    title = Column(String(2000))
    url = Column(String(2000))
    epsg = Column(String(100))
    spatial = Column(String(2000))
    description = Column(String(4000))
    attributes = Column(String(20000))
    timestamp = Column(String(2000))

    def __init__(self, type, group, title, url, epsg, spatial, description, attributes, timestamp):
        self.type = type
        self.group = group
        self.title = title
        self.url = url
        self.epsg = epsg
        self.spatial = spatial
        self.description = description
        self.attributes = attributes
        self.timestamp = timestamp


class Groups(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True)  # Record number.
    name = Column(String(100))
    description = Column(String(2000))

    def __init__(self, name, description):
        self.name = name
        self.description = description
