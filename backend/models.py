from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Pipeline(Base):
    __tablename__ = "pipelines"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    target_db = Column(String)
    recommendations = Column(Text)  # можно хранить в формате JSON-строки
    ddl_script = Column(Text)
    etl_code = Column(Text)
    created_at = Column(DateTime)
    hypothesis = Column(Text)
    source_path = Column(Text)


