import os
from datetime import datetime

from google.cloud import bigquery

local_vars = locals()


class CoinsPricesValidation:
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

    def validation_query(self):
        return f"""
        WITH 
        validation_fields as (
            SELECT DISTINCT 
                IF(snapshot_date = DATE('{self.partition_date}'), True, False) valid_snapshot_date,
                IF(id IN ("bitcoin", "ethereum"), True, False) valid_id,
                IF(symbol IN ("btc", "eth"), True, False) valid_symbol,
                IF(name IN ("Bitcoin", "Ethereum"), True, False) valid_name,
                IF(current_price_usd >= 0, True, False) valid_current_price_usd,
                IF(current_price_eur >= 0, True, False) valid_current_price_eur,
                IF(current_price_brl >= 0, True, False) valid_current_price_brl,
            FROM `{self.table}`
            WHERE day = DATE('{self.partition_date}')
        ),

        rows_with_problem as (
            SELECT 
                *
            FROM validation_fields
            WHERE
                not valid_snapshot_date or
                not valid_id or,
                not valid_symbol or,
                not valid_name or
                not valid_current_price_usd or
                not valid_current_price_eur or 
                not valid_current_price_brl             
        )

        SELECT * FROM objects_with_problem
        """

    def create_bq_client(self):
        return bigquery.Client(
            project=self.bq_project,
        )

    def query_result(self):
        query = self.client_bq.query(self.validation_query(),
                                     job_config=bigquery.QueryJobConfig(use_legacy_sql=False)
                                     )
        return query.result().to_dataframe()

    def validate(self):
        df = self.query_result()
        if df.size:
            for index, row in df:
                print(f"Invalid value on ({index}): {row}")
            raise Exception("Invalid value.")

