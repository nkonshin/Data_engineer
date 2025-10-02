# Data Engineer MVP

Проект «Data Engineer MVP» — веб-сервис для анализа структур данных и генерации ETL-пайплайнов.

## Описание

Сервис позволяет:
- Загрузить данные (CSV, JSON, XML) или подключиться к источнику (Postgres/ClickHouse).
- На основе первых строк данных LLM рекомендует целевую СУБД, структуру хранения, генерирует ETL-пайплайн (Airflow DAG), DDL-скрипт и краткий бизнес-отчёт.
- Пользователь может просмотреть рекомендации, подтвердить загрузку в выбранную СУБД, скачать отчёт/DDL.
- Можно управлять списком созданных пайплайнов (просмотр, загрузка, удаление).

## Требования

- Python 3.9+
- Docker и Docker Compose
- Node.js (только если запускать фронтенд вне Docker)
- LM Studio (локальная LLM, OpenAI-совместимый сервер)
  - Модель: qwen3-coder-30b-a3b-instruct-mlx (или совместимая)
- macOS на Apple Silicon (M1/M2): для Airflow/HDFS используется эмуляция linux/amd64 (может стартовать медленнее)

## Локальный запуск

1. Создайте виртуальное окружение (Windows PowerShell):

```
cd C:\DataAI\data_engineer
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

2. Установите зависимости:

```
pip install -r backend/requirements.txt
```

3. Запустите бэкенд:

```
python run.py
```

Бэкенд будет доступен по адресу `http://localhost:8000`.

3. Запустите фронтенд:

```
cd frontend
yarn install
yarn start
```

Фронтенд доступен по адресу `http://localhost:3000`.

## Запуск через Docker

1. Установите Docker и Docker Compose.
2. Запустите LM Studio и включите OpenAI-совместимый сервер:
   - Модель: `qwen3-coder-30b-a3b-instruct-mlx`
   - Эндпоинт: `http://localhost:1234/v1`
3. В корне проекта поднимите сервисы:

```
docker-compose up --build -d
```

4. Инициализируйте Airflow (один раз) ВНУТРИ контейнера и создайте пользователя:

```
docker exec -it airflow bash -lc "airflow db init"
docker exec -it airflow bash -lc \
  "airflow users create --username admin --password admin --firstname A --lastname D --role Admin --email admin@example.com"
docker-compose restart airflow
```

5. Откройте в браузере:
- Фронтенд: `http://localhost:3000`
- FastAPI (API): `http://localhost:8000`
- Airflow: `http://localhost:8080` (логин: `admin` / `admin`)
- HDFS NameNode UI: `http://localhost:9870`

Если PowerShell блокирует активацию venv:

```
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Использование

- Загрузите тестовый файл: `data/sample.csv`, `data/sample.json`, `data/sample.xml` (или свой файл).
- На главной странице выберите файл и нажмите «Анализировать и создать пайплайн».
- Перейдите в «Список пайплайнов»:
  - Раскройте «DDL» и «DAG (код)», при необходимости нажмите «Скачать DDL».
  - «Загрузить в БД» — зальёт данные в целевую СУБД (имя таблицы = имя файла, нормализовано).
  - «Сформировать гипотезу» — краткий бизнес-отчёт по данным (LLM).
  - «Загрузить в HDFS» — отправка исходного файла в HDFS (опционально).

## Структура проекта

- `run.py` — запуск FastAPI.
- `rundocker` — скрипт для Docker.
- `docker-compose.yml` — сервисы: backend, frontend, Airflow, ClickHouse, PostgreSQL, HDFS.
- `backend/` — FastAPI приложение (логика, модели, utils).
- `frontend/` — React приложение (интерфейс).
- `prompts/` — шаблоны промтов для LLM.
- `data/` — пример данных для тестирования.
- `tests/` — тесты PyTest.

## Тестирование

```
pytest
```

## Сервисы и порты

- Backend (FastAPI): `http://localhost:8000`
- Frontend (React): `http://localhost:3000`
- Airflow Web UI: `http://localhost:8080`
- ClickHouse HTTP: `http://localhost:8123`
- PostgreSQL: `localhost:5432`
- HDFS NameNode UI: `http://localhost:9870`

## Переменные окружения (backend)

Заданы в `docker-compose.yml` (сервис `backend`):

- `LLM_ENABLED=1`
- `OPENAI_BASE_URL=http://host.docker.internal:1234/v1`
- `OPENAI_MODEL=qwen3-coder-30b-a3b-instruct-mlx`
- `OPENAI_API_KEY=lm-studio` (заглушка, LM Studio обычно не требует)
- `PG_CONN_STR=postgresql://data_engineer:password@postgres:5432/data_engineer_db`
- `CLICKHOUSE_HOST=clickhouse`, `CLICKHOUSE_PORT=8123`, `CLICKHOUSE_DB=default`
- `HDFS_WEBHDFS=http://namenode:9870`, `HDFS_USER=root`

Примечание: текущая загрузка в ClickHouse предполагает доступ без авторизации. Для защищённого кластера настройте пользователя/пароль и адаптируйте код клиента.

## Демо‑сценарий (UI)

1) Загрузка CSV/JSON/XML → «Анализировать и создать пайплайн» → появится пайплайн.
2) В «Список пайплайнов»: просмотрите DDL и DAG → «Загрузить в БД».
3) Проверка в ClickHouse (пример для таблицы из `data/sample.csv` ⇒ `sample`):

```
curl -u default: "http://localhost:8123/?query=SHOW%20TABLES"
curl -u default: "http://localhost:8123/?query=SELECT%20count()%20FROM%20sample"
```

## ClickHouse аутентификация

Если HTTP-запросы требуют пользователя/пароль:

```
# Создать пользователя и выдать права (в контейнере):
docker exec -it clickhouse clickhouse-client -q "CREATE USER IF NOT EXISTS app IDENTIFIED WITH plaintext_password BY 'app'; GRANT ALL ON default.* TO app;"

# Пример запроса
curl -u app:app "http://localhost:8123/?query=SHOW%20TABLES"
```

## Нормализация имени таблицы

Имя таблицы формируется из имени файла и нормализуется: строчные буквы, пробелы и спецсимволы → `_`, множественные `_` схлопываются, если имя начинается с цифры — добавляется префикс `t_`.

## HDFS (опционально)

- Используются образы BDE (`bde2020/hadoop-*`) с `platform: linux/amd64` для совместимости на macOS (M1/M2).
- Загрузка файла: кнопка «Загрузить в HDFS» в UI.
- UI NameNode: `http://localhost:9870`.

## Траблшутинг

- Airflow пишет «You need to initialize the database» — выполните внутри контейнера:
  ```
  docker exec -it airflow bash -lc "airflow db init"
  docker exec -it airflow bash -lc "airflow users create --username admin --password admin --firstname A --lastname D --role Admin --email admin@example.com"
  docker-compose restart airflow
  ```
- LM Studio не доступен из контейнера — проверьте, что в `backend` задан `extra_hosts: host.docker.internal:host-gateway` и сервер LLM слушает `localhost:1234`.
- ClickHouse HTTP «Authentication failed» — используйте `-u default:` (пустой пароль) или создайте пользователя `app/app` (см. выше).

## Архитектура

```mermaid
flowchart LR
  subgraph Frontend [React]
    UI[UploadForm / PipelineList]
  end

  subgraph Backend [FastAPI]
    UP[Upload File]
    RECS[LLM Analyze\n(Recommendations/DDL/DAG/Hypothesis)]
    LOAD[Load to DB]
    HDFS[Upload to HDFS]
  end

  subgraph Infra [Docker]
    CH[(ClickHouse)]
    PG[(PostgreSQL)]
    AF[Airflow]
    H[(HDFS)]
    LLM[LM Studio]
  end

  UI -->|/upload-file| UP --> RECS --> AF
  UI -->|/pipelines| Backend
  UI -->|/load-to-db| LOAD --> CH & PG
  UI -->|/upload-to-hdfs| HDFS --> H
  Backend -->|OpenAI API| LLM
  Backend --> CH
  Backend --> PG
  AF -->|DAGs| Backend
```


