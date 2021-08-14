from google.cloud import bigquery
import requests
import pandas as pd
from datetime import datetime


class CoinsPrices:
    def __init__(self):
        now = datetime.today()
        self.partition_date = now.strftime("%Y-%m-%d")
        self.partition_date_no_dash = now.strftime("%Y%m%d")

        self.coins_ids = ["bitcoin", "ethereum"]
        self.info_fields = ["id", "symbol", "name"]
        self.bq_project = "data-case-study-322621"
        self.table = f"{self.bq_project}.rodrigohaddad.crypto_currency"

        self.client_bq = self.create_bq_client()

    def get_coins_price(self, coin_id: str):
        endpoint = f"coins/{coin_id}/history"
        http_response = requests.get(url=f"https://api.coingecko.com/api/v3/{endpoint}",
                                     params={"date": self.partition_date, "location": False})
        coin_info = http_response.json()

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
