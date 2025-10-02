import json
import os
from typing import Optional, Dict, Any

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

# LLM setup (Auto classes)
_LLM_MODEL_ID = os.environ.get("LLM_MODEL_ID", "Qwen/Qwen2.5-1.5B-Instruct")
_LLM_ENABLED = os.environ.get("LLM_ENABLED", "0") == "1"
_llm_model = None
_llm_tokenizer = None
try:
    from transformers import AutoTokenizer, AutoModelForCausalLM  # type: ignore
    import torch  # type: ignore
    if _LLM_ENABLED:
        _llm_tokenizer = AutoTokenizer.from_pretrained(_LLM_MODEL_ID)
        _llm_model = AutoModelForCausalLM.from_pretrained(_LLM_MODEL_ID)
except Exception:
    _llm_model = None
    _llm_tokenizer = None

def _read_prompt(path: str, **kwargs) -> str:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            template = f.read()
        return template.format(**kwargs)
    except Exception:
        return ""


def _llm_generate_json(prompt: str) -> Dict[str, Any]:
    if not (_llm_model and _llm_tokenizer):
        return {}
    try:
        inputs = _llm_tokenizer(prompt, return_tensors="pt")
        with torch.no_grad():
            outputs = _llm_model.generate(
                **inputs, max_new_tokens=512, do_sample=False
            )
        text = _llm_tokenizer.decode(outputs[0], skip_special_tokens=True)
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            payload = text[start:end+1]
            return json.loads(payload)
    except Exception:
        return {}
    return {}


def analyze_structure(sample_data):
    """
    Анализ структуры данных и рекомендация целевой СУБД.
    Вызывает LLM с соответствующим промтом (эмуляция).
    """
    prompt = _read_prompt(os.path.join("prompts", "analyze_structure.txt"), data=json.dumps(sample_data, ensure_ascii=False))
    llm = _llm_generate_json(prompt) if prompt else {}
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


def generate_etl_pipeline(sample_data, target_db, dag_id: str = "etl_pipeline"):
    dag_code = f"""from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract():
    print("Извлекаем данные")

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
    schedule_interval='0 * * * *',
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
    llm = _llm_generate_json(prompt) if prompt else {}
    if llm and isinstance(llm.get("hypothesis"), str):
        return llm["hypothesis"]
    # fallback
    return (
        "Данные по продажам могут помочь в анализе сезонных трендов и прогнозировании доходов. "
        "Рекомендуется создать витрину данных для аналитиков и настроить регулярное обновление."
    )


def generate_ddl(sample_data):
    prompt = _read_prompt(os.path.join("prompts", "generate_ddl.txt"), data=json.dumps(sample_data, ensure_ascii=False))
    llm = _llm_generate_json(prompt) if prompt else {}
    if llm and isinstance(llm.get("ddl"), str):
        return llm["ddl"]
    if not sample_data:
        return ""
    columns = sample_data[0].keys()
    # PostgreSQL
    ddl_pg = "CREATE TABLE IF NOT EXISTS dataset (\n"
    for col in columns:
        ddl_pg += f"    {col} TEXT,\n"
    ddl_pg = ddl_pg.rstrip(",\n") + "\n);\n"
    ddl_pg += "-- Рекомендуется создать индексы по ключевым полям\n"
    # ClickHouse
    ddl_ch_cols = ",\n".join([f"    `{c}` String" for c in columns])
    ddl_ch = (
        "CREATE TABLE IF NOT EXISTS dataset_ch (\n" +
        ddl_ch_cols +
        "\n) ENGINE = MergeTree\nPARTITION BY toYYYYMMDD(parseDateTimeBestEffort(date))\nORDER BY tuple();\n"
    )
    return ddl_pg + "\n" + ddl_ch


def load_to_target_db(etl_code, target_db):
    print(f"Загрузка данных в {target_db} выполнена.")
    return "success"


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
    client.command(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols}) ENGINE = MergeTree ORDER BY tuple()")
    client.insert(table_name, df.values.tolist(), column_names=list(df.columns))

def upload_to_hdfs(local_path: str, hdfs_path: str) -> bool:
    return upload_file_to_hdfs(local_path, hdfs_path)


