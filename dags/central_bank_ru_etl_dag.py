import clickhouse_connect
from airflow.sdk import DAG, task
from datetime import datetime, timedelta
from requests import request
from xmltodict import parse


CH_CONFIG = {
    "host": "clickhouse",
    "port": 8123,
    "username": "de_user",
    "password": "de_password",
    "database": "currency_dwh",
}

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="ru_central_bank_etl_pipeline",
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    tags={"pet-project", "clickhouse", "etl", "cbr"},
    catchup=False,
    max_active_runs=1,
    default_args=
    {
        "retries": 3,
        "retry_delay": timedelta(minutes=2)
    }
) :
    @task
    def fetch_cbr_data(ds=None):
        response = fetch(ds)
        currency_rates = parse_currency_rates(response.text)
        return currency_rates


    def fetch(ds=None):
        url = "https://www.cbr.ru/scripts/XML_daily.asp"

        if ds is not None:
            data_req = ds.replace("-", "/")
            parameters = {"data_req": data_req}
            response = request(method="get", url=url, params=parameters)
        else:
            response = request(method="get", url=url)

        return response


    def parse_currency_rates(text) -> list[dict]:
        currency_rates_raw = parse(text)
        currency_rates = []

        date = currency_rates_raw['ValCurs']['@Date']
        for currency_rate_raw in currency_rates_raw['ValCurs']['Valute']:
            currency_rate = {
                'currency_identifier': currency_rate_raw['@ID'],
                'symbol': currency_rate_raw['CharCode'],
                'nominal': int(currency_rate_raw['Nominal']),
                'currency_name': currency_rate_raw['Name'],
                'currency_value': float(currency_rate_raw['Value'].replace(',', '.')),
                "currency_value_unit_rate": float(currency_rate_raw['VunitRate'].replace(',', '.')),
                "load_date": datetime.strptime(date, "%d.%m.%Y")
            }
            currency_rates.append(currency_rate)
        return currency_rates

    @task
    def load_currency_rates_to_clickhouse(currency_rates, ds: str = None):

        client = clickhouse_connect.get_client(**CH_CONFIG)
        load_date = datetime.strptime(ds, "%Y-%m-%d").date()

        client.command(
            f"ALTER TABLE currency_dwh.raw_rates DELETE WHERE load_date =  toDateTime('{ds}') SETTINGS mutations_sync = 1"
        )

        rows = []

        for currency_curse in currency_rates:
            row = [
                currency_curse["currency_identifier"],
                currency_curse["symbol"],
                currency_curse["nominal"],
                currency_curse["currency_name"],
                currency_curse["currency_value"],
                currency_curse["currency_value_unit_rate"],
                currency_curse["load_date"],
            ]

            rows.append(row)

        client.insert(
            "currency_dwh.raw_rates",
            rows,
            column_names=[
                "currency_identifier",
                "symbol",
                "nominal",
                "currency_name",
                "currency_value",
                "currency_unit_rate",
                "load_date"
            ]
        )
        return len(rows)

    @task
    def data_quality_check(inserted_count: int, ds: str = None):
        if inserted_count == 0:
            raise ValueError(f"DQ Failed: API вернул 0 записей за {ds}")

        client = clickhouse_connect.get_client(**CH_CONFIG)
        result = client.query(
            f"SELECT count(), countIf(currency_value_unit_rate <= 0) FROM "
        )
        total_rows, bad_currency_value_unit_rate = result.first_row

        if total_rows != inserted_count:
            raise ValueError(f"DQ Failed: Ожидалось {inserted_count} строк, в БД найдено {total_rows}")
        if bad_currency_value_unit_rate > 0:
            raise ValueError(f"DQ Failed: Найдено {bad_currency_value_unit_rate} записей с некорректной ценой <= 0")

    @task
    def build_data_mart(ds: str = None):
        client = clickhouse_connect.get_client(**CH_CONFIG)

        client.command(
            f"ALTER TABLE currency_dwh.currency_metrics DELETE WHERE load_date =  toDateTime('{ds}') SETTINGS mutations_sync = 1"
        )

        query = f"""
        INSERT INTO currency_dwh.currency_metrics
        SELECT 
            currency_identifier,
            symbol,
            nominal,
            currency_name,
            currency_value,
            previous_currency_value,
            if (previous_currency_value > 0, round((currency_value - previous_currency_value) / previous_currency_value * 100, 2), 0) AS daily_change,
            round(moving_avg_7d, 4) AS moving_avg_7d,
            load_date
        FROM (
            SELECT
                currency_identifier,
                symbol,
                nominal,
                currency_name,
                currency_value,
                lagInFrame(currency_value, 1, currency_value) OVER (
                    PARTITION BY currency_identifier ORDER BY load_date
                ) AS previous_currency_value,
                avg(currency_value) OVER (
                    PARTITION BY currency_identifier ORDER BY load_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS moving_avg_7d,
                load_date
            FROM currency_dwh.raw_rates FINAL
        )
        WHERE load_date = '{ds}'
        """
        client.command(query)


    cbr_data = fetch_cbr_data()
    load_task = load_currency_rates_to_clickhouse(cbr_data)
    load_task >> build_data_mart()

