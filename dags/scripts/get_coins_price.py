from google.cloud import bigquery
import requests
import pandas as pd
from datetime import datetime
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

local_vars = locals()


class CoinsPrices:
    def __init__(self):
        config = {**local_vars}
        config.update({k.lower(): v for k, v in os.environ.items()})

        now = datetime.today()
        ctx = config.get("airflow_ctx_execution_date", now.strftime("%Y-%m-%d"))
        ctx_date = datetime.strptime(ctx[:10], "%Y-%m-%d")
        self.partition_date = ctx_date.strftime("%Y-%m-%d")
        self.partition_date_no_dash = ctx_date.strftime("%Y%m%d")
        self.date_coin = ctx_date.strftime("%d-%m-%Y")

        self.coins_ids = ["bitcoin", "ethereum"]
        self.info_fields = ["id", "symbol", "name"]
        self.bq_project = "data-case-study-322621"
        self.table = f"{self.bq_project}.rodrigohaddad.crypto_currency"

        self.client_bq = self.create_bq_client()

        self.http_adapter = self.get_adapter()

    @staticmethod
    def get_adapter():
        retry_strategy = Retry(
            total=3,
            backoff_factor=30
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        return http

    def get_coins_price(self, coin_id: str) -> pd.DataFrame:
        endpoint = f"coins/{coin_id}/history"
        try:
            http_response = self.http_adapter.get(url=f"https://api.coingecko.com/api/v3/{endpoint}",
                                                  params={"date": self.date_coin, "location": False})
            coin_info = http_response.json()
        except:
            return self.empty_row_for_error()

        row = [
            {
                "id": coin_info["id"],
                "symbol": coin_info["symbol"],
                "name": coin_info["name"],
                "snapshot_date": datetime.strptime(self.partition_date, '%Y-%m-%d').date(),
                "current_price_usd": coin_info["market_data"]["current_price"]["usd"],
                "current_price_eur": coin_info["market_data"]["current_price"]["eur"],
                "current_price_brl": coin_info["market_data"]["current_price"]["brl"],
            }
        ]

        return pd.DataFrame(row)

    def empty_row_for_error(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "id": "",
                "symbol": "",
                "name": "",
                "snapshot_date": datetime.strptime(self.partition_date, '%Y-%m-%d').date(),
                "current_price_usd": 0.0,
                "current_price_eur": 0.0,
                "current_price_brl": 0.0,
            }
        ])

    def create_bq_client(self):
        return bigquery.Client(
            project=self.bq_project,
        )

    def load_result_to_bq(self, df: pd.DataFrame, client_bq):
        job = client_bq.load_table_from_dataframe(
            df,
            f"{self.table}${self.partition_date_no_dash}",
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                schema_update_options=[
                    "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
                ],
                schema=[
                    bigquery.SchemaField("snapshot_date", "DATE"),
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("symbol", "STRING"),
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("current_price_usd", "FLOAT"),
                    bigquery.SchemaField("current_price_eur", "FLOAT"),
                    bigquery.SchemaField("current_price_brl", "FLOAT"),
                ],
                time_partitioning=bigquery.table.TimePartitioning(field="snapshot_date")
            )
        )

        job.result()

    def process(self):
        df_results = pd.DataFrame()
        for coin_id in self.coins_ids:
            df_results = pd.concat([df_results, self.get_coins_price(coin_id)])

        self.load_result_to_bq(df_results, self.client_bq)


def main():
    coins_price = CoinsPrices()
    coins_price.process()


if __name__ == '__main__':
    main()
