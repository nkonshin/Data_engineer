import json
import os
from typing import Optional, Dict, Any
import requests
import pandas as pd
import re

# Optional third-party clients (imported at top per guideline)
try:
    from sqlalchemy import create_engine  # type: ignore
except Exception:
    create_engine = None  # type: ignore

try:
    from clickhouse_connect import get_client  # type: ignore
except Exception:
    get_client = None  # type: ignore

try:
    from hdfs import InsecureClient  # type: ignore
except Exception:
    InsecureClient = None  # type: ignore

# LLM setup (OpenAI-compatible, e.g., LM Studio)
_LLM_ENABLED = os.environ.get("LLM_ENABLED", "0") == "1"
_OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://host.docker.internal:1234/v1")
_OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "qwen3-coder-30b-a3b-instruct-mlx")
_OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "lm-studio")

def _llm_chat_json(system_text: str, user_text: str) -> Dict[str, Any]:
    if not _LLM_ENABLED:
        return {}
    try:
        url = f"{_OPENAI_BASE_URL.rstrip('/')}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_OPENAI_API_KEY}",
        }
        payload = {
            "model": _OPENAI_MODEL,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_text},
            ],
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        # Строгая попытка распарсить JSON
        try:
            return json.loads(content)
        except Exception:
            # Фолбек: вырезать JSON из текста
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end != -1 and end > start:
                return json.loads(content[start:end+1])
            return {}
    except Exception:
        return {}

def _read_prompt(path: str, **kwargs) -> str:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            template = f.read()
        return template.format(**kwargs)
    except Exception:
        return ""


def _llm_generate_json(prompt: str) -> Dict[str, Any]:
    # Backward-compat: вызывался ранее — перенаправим в чат-модель
    system_text = (
        "Ты ИИ-ассистент дата-инженера. Всегда отвечай строго валидным JSON без текста вокруг."
    )
    return _llm_chat_json(system_text, prompt)


def analyze_structure(sample_data):
    """
    Анализ структуры данных и рекомендация целевой СУБД.
    Вызывает LLM с соответствующим промтом (эмуляция).
    """
    prompt = _read_prompt(os.path.join("prompts", "analyze_structure.txt"), data=json.dumps(sample_data, ensure_ascii=False))
    system_text = (
        "Ты ИИ-ассистент дата-инженера. Верни ТОЛЬКО один JSON. Ключи: "
        "target_db|recommendations|ddl|hypothesis|etl."
    )
    llm = _llm_chat_json(system_text, prompt) if prompt else {}
    if llm:
        # ожидаемая структура: target_db, recommendations, ddl, hypothesis, etl
        return llm
    # Фолбек-правила
    fields = list(sample_data[0].keys()) if sample_data else []
    has_date = any("date" in f.lower() or "time" in f.lower() for f in fields)
    target = "clickhouse" if has_date else "postgres"
    return {
        "target_db": target,
        "recommendations": "Разделение по дате" if has_date else "",
        "ddl": generate_ddl(sample_data),
        "hypothesis": generate_hypothesis({"target_db": target}),
        "etl": generate_etl_pipeline(sample_data, target_db=target),
    }


def generate_recommendations(sample_data):
    return {}


def generate_etl_pipeline(sample_data, target_db, dag_id: str = "etl_pipeline", source_path: Optional[str] = None, schedule_cron: str = "0 * * * *"):
    src = source_path or "<provided at runtime>"
    dag_code = f"""from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract():
    print("Извлекаем данные из: {src}")

def transform():
    print("Трансформируем данные")

def load():
    print("Загружаем данные в {target_db}")

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='ETL pipeline to {target_db}',
    schedule_interval='{schedule_cron}',
    start_date=days_ago(1),
    tags=['etl'],
) as dag:
    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    t1 >> t2 >> t3
"""
    return dag_code


def generate_hypothesis(recommendations):
    prompt = _read_prompt(os.path.join("prompts", "generate_hypothesis.txt"), recommendations=json.dumps(recommendations, ensure_ascii=False))
    system_text = "Ты пишешь краткие бизнес-гипотезы на русском. Верни ТОЛЬКО JSON: {\"hypothesis\": string}."
    llm = _llm_chat_json(system_text, prompt) if prompt else {}
    if llm and isinstance(llm.get("hypothesis"), str):
        return llm["hypothesis"]
    # fallback
    return (
        "Данные по продажам могут помочь в анализе сезонных трендов и прогнозировании доходов. "
        "Рекомендуется создать витрину данных для аналитиков и настроить регулярное обновление."
    )


def generate_ddl(sample_data):
    prompt = _read_prompt(os.path.join("prompts", "ddl.txt"), data=json.dumps(sample_data, ensure_ascii=False))
    system_text = "Ты генерируешь DDL для PostgreSQL и ClickHouse. Верни JSON: {\"ddl\": string}."
    llm = _llm_chat_json(system_text, prompt) if prompt else {}
    if llm and isinstance(llm.get("ddl"), str):
        return llm["ddl"]
    # Fallback: типизация по образцу
    if not sample_data:
        return ""
    row = sample_data[0]
    def infer_pg_type(val):
        if isinstance(val, bool):
            return "BOOLEAN"
        if isinstance(val, int):
            return "BIGINT"
        if isinstance(val, float):
            return "DOUBLE PRECISION"
        if isinstance(val, str):
            v = val.strip()
            # простая эвристика даты/времени
            if any(s in v for s in ["-", ":", "T"]) and len(v) >= 8:
                return "TIMESTAMP"
            return "TEXT"
        return "TEXT"
    def infer_ch_type(val):
        if isinstance(val, bool):
            return "UInt8"
        if isinstance(val, int):
            return "Int64"
        if isinstance(val, float):
            return "Float64"
        if isinstance(val, str):
            v = val.strip()
            if any(s in v for s in ["-", ":", "T"]) and len(v) >= 8:
                return "DateTime"
            return "String"
        return "String"
    columns = list(row.keys())
    # PG DDL
    ddl_pg_cols = []
    for c in columns:
        sample_val = row[c]
        ddl_pg_cols.append(f"    \"{c}\" {infer_pg_type(sample_val)}")
    ddl_pg = "CREATE TABLE IF NOT EXISTS dataset (\n" + ",\n".join(ddl_pg_cols) + "\n);\n"
    ddl_pg += "-- Рекомендуются индексы по полям фильтрации/джойнов\n"
    # CH DDL
    ddl_ch_cols = []
    has_ts = None
    for c in columns:
        sample_val = row[c]
        t = infer_ch_type(sample_val)
        ddl_ch_cols.append(f"    `{c}` {t}")
        if t == "DateTime" and has_ts is None:
            has_ts = c
    partition = f"PARTITION BY toDate({has_ts})\n" if has_ts else ""
    order_by = f"ORDER BY ({has_ts})" if has_ts else "ORDER BY tuple()"
    ddl_ch = (
        "CREATE TABLE IF NOT EXISTS dataset_ch (\n" +
        ",\n".join(ddl_ch_cols) +
        f"\n) ENGINE = MergeTree\n{partition}{order_by};\n"
    )
    return ddl_pg + "\n" + ddl_ch


def load_to_target_db(source_path: str, target_db: str):
    """Простая загрузка данных из локального файла в выбранную СУБД."""
    if not source_path or not os.path.exists(source_path):
        return "source_not_found"
    # Чтение файла
    ext = os.path.splitext(source_path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(source_path)
        elif ext == ".json":
            df = pd.read_json(source_path)
        elif ext == ".xml":
            try:
                df = pd.read_xml(source_path)
            except Exception:
                # Фолбек: попробовать через xmltodict
                try:
                    import xmltodict  # type: ignore
                    with open(source_path, 'r', encoding='utf-8') as f:
                        data = xmltodict.parse(f.read())
                    # попытка найти список записей
                    if isinstance(data, dict):
                        # возьмём первый список словарей
                        records = None
                        for v in data.values():
                            if isinstance(v, list) and v and isinstance(v[0], dict):
                                records = v
                                break
                            if isinstance(v, dict):
                                for vv in v.values():
                                    if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                                        records = vv
                                        break
                        if records is None:
                            return "xml_parse_failed"
                        df = pd.DataFrame(records)
                    else:
                        return "xml_parse_failed"
                except Exception:
                    return "xml_parse_failed"
        else:
            return "unsupported_format"
    except Exception:
        return "read_failed"

    table = os.path.splitext(os.path.basename(source_path))[0]
    table = _sanitize_table_name(table)
    if target_db == "postgres":
        conn = os.environ.get("PG_CONN_STR", "postgresql://data_engineer:password@postgres:5432/data_engineer_db")
        try:
            upload_to_postgres(df, table_name=table, conn_str=conn)
            return "success"
        except Exception:
            return "pg_upload_failed"
    elif target_db == "clickhouse":
        try:
            upload_to_clickhouse(df, table_name=table)
            return "success"
        except Exception:
            return "ch_upload_failed"
    else:
        return "unsupported_target"


def upload_file_to_hdfs(local_path: str, hdfs_path: str) -> bool:
    if InsecureClient is None:
        raise RuntimeError("hdfs client is not installed")
    hdfs_host = os.environ.get("HDFS_WEBHDFS", "http://namenode:9870")
    client = InsecureClient(hdfs_host, user=os.environ.get("HDFS_USER", "root"))
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)
    with open(local_path, 'rb') as reader:
        client.write(hdfs_path, reader, overwrite=True)
    return True


def upload_to_postgres(df, table_name: str, conn_str: str) -> None:
    if create_engine is None:
        raise RuntimeError("sqlalchemy not installed")
    engine = create_engine(conn_str)
    df.to_sql(table_name, engine, if_exists='append', index=False)


def upload_to_clickhouse(df, table_name: str, host: Optional[str] = None, port: Optional[int] = None, database: Optional[str] = None):
    if get_client is None:
        raise RuntimeError("clickhouse-connect not installed")
    host = host or os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = int(port or os.environ.get("CLICKHOUSE_PORT", 8123))
    database = database or os.environ.get("CLICKHOUSE_DB", "default")
    client = get_client(host=host, port=port, database=database)
    cols = ", ".join([f"`{c}` String" for c in df.columns])
    safe_table = _sanitize_table_name(table_name)
    client.command(f"CREATE TABLE IF NOT EXISTS `{safe_table}` ({cols}) ENGINE = MergeTree ORDER BY tuple()")
    client.insert(safe_table, df.values.tolist(), column_names=list(df.columns))
    
def upload_to_hdfs(local_path: str, hdfs_path: str) -> bool:
    return upload_file_to_hdfs(local_path, hdfs_path)


def _sanitize_table_name(name: str) -> str:
    """Приводит имя таблицы к безопасному формату: латиница/цифры/подчёркивание, в нижнем регистре.
    Не начинается с цифры (в этом случае добавляется префикс t_)."""
    # заменить всё, кроме букв/цифр, на подчёркивание
    s = re.sub(r"[^A-Za-z0-9_]", "_", name)
    # сжать повторяющиеся подчёркивания
    s = re.sub(r"_+", "_", s)
    # обрезать подчёркивания по краям
    s = s.strip("_")
    # в нижний регистр
    s = s.lower() or "table"
    # не начинать с цифры
    if s[0].isdigit():
        s = f"t_{s}"
    return s
