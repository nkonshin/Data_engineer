import pytest
from backend.database import engine
from backend.models import Base, Pipeline
from sqlalchemy.orm import Session
from datetime import datetime


def test_pipeline_db_entry():
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)
    pipeline = Pipeline(
        name="TestPipeline",
        target_db="postgres",
        recommendations='{}',
        ddl_script="CREATE TABLE test (id INT);",
        etl_code="print('Hello')",
        created_at=datetime.utcnow()
    )
    session.add(pipeline)
    session.commit()
    assert pipeline.id is not None
    session.delete(pipeline)
    session.commit()
    session.close()


