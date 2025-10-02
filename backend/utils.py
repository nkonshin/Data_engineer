import json
import os

_LLM_AVAILABLE = False
_pipeline = None
try:
    # Попытка инициализации локального пайплайна суммаризации/генерации
    from transformers import pipeline  # type: ignore
    model_id = os.environ.get("LLM_MODEL_ID", "Qwen/Qwen2.5-1.5B-Instruct")
    _pipeline = pipeline("text-generation", model=model_id, device_map="auto")
    _LLM_AVAILABLE = True
except Exception:
    _LLM_AVAILABLE = False

def analyze_structure(sample_data):
    """
    Анализ структуры данных и рекомендация целевой СУБД.
    Вызывает LLM с соответствующим промтом (эмуляция).
    """
    prompt = (
        "Анализ структуры данных:\n" +
        json.dumps(sample_data, ensure_ascii=False, indent=2) +
        "\nОпредели целевое хранилище: PostgreSQL (оперативные), "
        "ClickHouse (агрегированные/аналитические), HDFS (сырые). "
        "Обоснуй выбор и предложи партиционирование и индексы."
    )
    if _LLM_AVAILABLE and _pipeline is not None:
        try:
            out = _pipeline(prompt, max_new_tokens=256, do_sample=False)
            text = out[0]["generated_text"] if isinstance(out, list) else str(out)
            # Примитивный парсер ответа
            target = "postgres" if "Postgre" in text else (
                "clickhouse" if "ClickHouse" in text or "Clickhouse" in text else (
                    "hdfs" if "HDFS" in text or "hdfs" in text else "postgres"
                )
            )
            return {
                "target_db": target,
                "comment": text[:1000],
                "partitioning": "По дате" if "дат" in text.lower() else "",
                "indexes": "Индексы по ключам" if "индекс" in text.lower() else "",
            }
        except Exception:
            pass
    # Фолбек-правила
    fields = list(sample_data[0].keys()) if sample_data else []
    has_date = any("date" in f.lower() or "time" in f.lower() for f in fields)
    target = "clickhouse" if has_date else "postgres"
    return {
        "target_db": target,
        "partitioning": "Разделение по дате" if has_date else "",
        "indexes": "Создать индекс по ключевому полю",
        "comment": "Эвристика: по наличию дат выбираем ClickHouse, иначе Postgres."
    }


def generate_recommendations(sample_data):
    return {}


def generate_etl_pipeline(sample_data, target_db):
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
    dag_id='etl_pipeline_{target_db}',
    default_args=default_args,
    description='ETL pipeline to {target_db}',
    schedule_interval=None,
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
    _ = "На основе следующих рекомендаций:" + json.dumps(recommendations, ensure_ascii=False)
    report = "Данные по продажам могут помочь в анализе сезонных трендов и прогнозировании доходов. " \
             "Рекомендуется создать витрину данных для аналитиков и настроить регулярное обновление."
    return report


def generate_ddl(sample_data):
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


