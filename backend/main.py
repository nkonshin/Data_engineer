from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import shutil, os
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
import json

from database import SessionLocal, engine
from models import Base, Pipeline
from utils import (
    analyze_structure,
    generate_recommendations,
    generate_etl_pipeline,
    generate_hypothesis,
    generate_ddl,
    load_to_target_db
)

# Инициализация базы данных
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Data Engineer MVP")

# Разрешаем CORS для фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineId(BaseModel):
    pipeline_id: int

@app.get("/")
async def root():
    return {"status": "API работает"}


@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    """
    Загрузка файла (CSV/JSON/XML), анализ структуры данных,
    генерация рекомендаций и DDL.
    """
    upload_dir = "data"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file.filename.endswith(".json"):
            df = pd.read_json(file_path)
        elif file.filename.endswith(".xml"):
            df = pd.read_xml(file_path)
        else:
            return JSONResponse(status_code=400, content={"error": "Неподдерживаемый формат файла"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Ошибка чтения файла: {e}"})

    df_nonzero = df.dropna(how='all').head(10)
    sample_data = df_nonzero.to_dict(orient='records')

    recs = analyze_structure(sample_data)
    # Если LLM вернула готовые поля
    ddl_script = recs.get("ddl") if isinstance(recs, dict) else None
    ddl_script = ddl_script or generate_ddl(sample_data)
    pipeline_code = recs.get("etl") if isinstance(recs, dict) else None
    pipeline_code = pipeline_code or generate_etl_pipeline(sample_data, target_db=recs.get("target_db", "postgres"))

    db: Session = SessionLocal()
    pipeline = Pipeline(
        name=f"Pipeline_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        target_db=recs.get("target_db", "postgres"),
        recommendations=json.dumps(recs, ensure_ascii=False),
        ddl_script=ddl_script,
        etl_code=pipeline_code,
        created_at=datetime.utcnow(),
        hypothesis=None,
        source_path=file_path
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    # Сохраняем DAG с уникальным id и обновляем запись
    dag_id = f"etl_pipeline_{pipeline.id}"
    dag_code = generate_etl_pipeline(sample_data, target_db=recs.get("target_db", "postgres"), dag_id=dag_id)
    dags_dir = os.path.join(os.path.dirname(__file__), 'airflow_dags')
    os.makedirs(dags_dir, exist_ok=True)
    dag_path = os.path.join(dags_dir, f"{dag_id}.py")
    with open(dag_path, 'w', encoding='utf-8') as f:
        f.write(dag_code)
    pipeline.etl_code = dag_code
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    db.close()

    return {"pipeline_id": pipeline.id, "recommendations": recs, "ddl": ddl_script}


@app.post("/connect-db/")
async def connect_db(
    source_db: str = Form(...),
    host: str = Form(...),
    port: int = Form(...),
    dbname: str = Form(...),
    user: str = Form(None),
    password: str = Form(None)
):
    """
    Подключение к источнику данных (PostgreSQL или ClickHouse),
    извлечение первых 10 ненулевых строк и генерация рекомендаций.
    """
    if source_db == "postgres":
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        try:
            df = pd.read_sql("SELECT * FROM data LIMIT 10;", conn_str)
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": f"Ошибка подключения к Postgres: {e}"})
    elif source_db == "clickhouse":
        try:
            from clickhouse_connect import get_client
            client = get_client(host=host, port=port)
            df = client.query_df(f"SELECT * FROM {dbname} LIMIT 10")
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": f"Ошибка подключения к ClickHouse: {e}"})
    else:
        return JSONResponse(status_code=400, content={"error": "Неподдерживаемый источник данных"})

    df_nonzero = df.dropna(how='all').head(10)
    sample_data = df_nonzero.to_dict(orient='records')

    recs = analyze_structure(sample_data)
    ddl_script = recs.get("ddl") if isinstance(recs, dict) else None
    ddl_script = ddl_script or generate_ddl(sample_data)
    pipeline_code = recs.get("etl") if isinstance(recs, dict) else None
    pipeline_code = pipeline_code or generate_etl_pipeline(sample_data, target_db=recs.get("target_db", "postgres"))

    db: Session = SessionLocal()
    pipeline = Pipeline(
        name=f"Pipeline_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        target_db=recs.get("target_db", "postgres"),
        recommendations=json.dumps(recs, ensure_ascii=False),
        ddl_script=ddl_script,
        etl_code=pipeline_code,
        created_at=datetime.utcnow(),
        hypothesis=None,
        source_path=f"{source_db}://{host}:{port}/{dbname}"
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    # Сохранение DAG
    dag_id = f"etl_pipeline_{pipeline.id}"
    dag_code = generate_etl_pipeline(sample_data, target_db=recs.get("target_db", "postgres"), dag_id=dag_id)
    dags_dir = os.path.join(os.path.dirname(__file__), 'airflow_dags')
    os.makedirs(dags_dir, exist_ok=True)
    dag_path = os.path.join(dags_dir, f"{dag_id}.py")
    with open(dag_path, 'w', encoding='utf-8') as f:
        f.write(dag_code)
    pipeline.etl_code = dag_code
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    db.close()

    return {"pipeline_id": pipeline.id, "recommendations": recs, "ddl": ddl_script}


@app.get("/pipelines/")
async def list_pipelines():
    db: Session = SessionLocal()
    pipelines = db.query(Pipeline).all()
    db.close()
    # Сериализация recommendations из JSON-строки
    results = []
    for p in pipelines:
        item = {
            "id": p.id,
            "name": p.name,
            "target_db": p.target_db,
            "recommendations": json.loads(p.recommendations) if p.recommendations else {},
            "ddl_script": p.ddl_script,
            "etl_code": p.etl_code,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        results.append(item)
    return results


@app.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: int):
    db: Session = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    db.close()
    if not pipeline:
        return JSONResponse(status_code=404, content={"error": "Пайплайн не найден"})
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "target_db": pipeline.target_db,
        "recommendations": json.loads(pipeline.recommendations) if pipeline.recommendations else {},
        "ddl_script": pipeline.ddl_script,
        "etl_code": pipeline.etl_code,
        "created_at": pipeline.created_at.isoformat() if pipeline.created_at else None,
    }


@app.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: int):
    db: Session = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        db.close()
        return JSONResponse(status_code=404, content={"error": "Пайплайн не найден"})
    db.delete(pipeline)
    db.commit()
    db.close()
    return {"status": "deleted"}


@app.post("/load-to-db/")
async def load_to_db(body: PipelineId):
    db: Session = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == body.pipeline_id).first()
    if not pipeline:
        db.close()
        return JSONResponse(status_code=404, content={"error": "Пайплайн не найден"})
    db.close()
    result = load_to_target_db(pipeline.etl_code, pipeline.target_db)
    return {"status": result}


@app.post("/hypothesis/")
async def generate_report(body: PipelineId):
    db: Session = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == body.pipeline_id).first()
    db.close()
    if not pipeline:
        return JSONResponse(status_code=404, content={"error": "Пайплайн не найден"})
    recs = json.loads(pipeline.recommendations) if pipeline.recommendations else {}
    report = generate_hypothesis(recs)
    # сохраняем гипотезу
    db: Session = SessionLocal()
    p = db.query(Pipeline).filter(Pipeline.id == body.pipeline_id).first()
    if p:
        p.hypothesis = report
        db.add(p)
        db.commit()
    db.close()
    return {"hypothesis": report}


class HdfsPayload(BaseModel):
    pipeline_id: int
    hdfs_path: str

@app.post("/upload-to-hdfs/")
async def upload_to_hdfs(payload: HdfsPayload):
    db: Session = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == payload.pipeline_id).first()
    db.close()
    if not pipeline:
        return JSONResponse(status_code=404, content={"error": "Пайплайн не найден"})
    # делегируем в util функцию загрузки файла в HDFS
    try:
        from utils import upload_file_to_hdfs
        ok = upload_file_to_hdfs(local_path=pipeline.source_path, hdfs_path=payload.hdfs_path)
        return {"status": "success" if ok else "failed"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


