# ETL-пайплайн курсов валют ЦБ РФ (Apache Airflow 3.4 + ClickHouse)

Учебный Data Engineering проект для автоматического сбора ежедневных и исторических курсов валют из XML API Центрального Банка РФ, их обработки и построения аналитической витрины в ClickHouse.

## Архитектура и стек технологий

* **Оркестрация:** Apache Airflow 3.4 (TaskFlow API, `LocalExecutor`)
* **Аналитическое хранилище (DWH):** ClickHouse 24.8 (`clickhouse-connect`)
* **База метаданных Airflow:** PostgreSQL 15
* **Контейнеризация:** Docker & Docker Compose

### Логика работы DAG (`ru_central_bank_etl_pipeline`)
1. **`fetch_cbr_data`** — запрашивает XML-данные с официального API ЦБ РФ за указанную дату логического запуска (`logical_date`) и парсит их в список словарей.
2. **`load_currency_rates_to_clickhouse`** — подключается к ClickHouse и загружает сырые курсы валют батчем.
3. **`build_data_mart`** — запускает SQL-запрос для обновления аналитической витрины данных.

---

## Структура проекта

```text
├── dags/
│   └── central_bank_ru_etl_dag.py   # Основной DAG-файл пайплайна
├── sql/
│   └── init_clickhouse.sql          # DDL-скрипты создания БД и таблиц ClickHouse
├── Dockerfile                       # Кастомный образ Airflow 3.4 с clickhouse-connect
├── docker-compose.yml               # Оркестрация сервисов (Airflow, Postgres, ClickHouse)
├── requirements.txt                 # Зависимости Python
└── README.md
```

---

