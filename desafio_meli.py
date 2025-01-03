import os
import json
import pandas as pd
import requests
import logging

from dotenv import load_dotenv
from pathlib import Path
from google.cloud import bigquery

# Cargar las variables del archivo .env
load_dotenv()

here = Path(__file__).parent


def read_json(config_path: Path) -> dict:
    """Lee un archivo de configuración JSON."""
    with open(config_path, "r") as file:
        config = json.load(file)
    return config


class DataBase:
    def __init__(self):
        config = read_json(here / "config.json")
        self.client = self._create_bigquery_client()
        self.dataset_id: str = config["database"]["dataset_id"]
        self.tables: dict = config["database"]["tables"]
        self.currency_schema = self.currency_schema()
        self.item_schema = self.item_schema()


    def _set_gcp_credentials(self):
        credentials_path = here / "gcp_credentials.json"
        print(credentials_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)


    def currency_schema(self) -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("currency_base", "STRING"),
            bigquery.SchemaField("currency_quote", "STRING"),
            bigquery.SchemaField("rate", "FLOAT"), 
        ]
    

    def item_schema(self) -> list[bigquery.SchemaField]:
        return [    
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("site_id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("currency_id", "STRING"),
            bigquery.SchemaField("seller_id", "STRING"),
            bigquery.SchemaField("category_id", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("sale_terms", "STRING"),
            bigquery.SchemaField("warrancy", "STRING"),
            bigquery.SchemaField("shipping", "STRING"),
        ]


    def _create_bigquery_client(self) -> bigquery.Client:
        # creacion de cliente en bigquery
        self._set_gcp_credentials()
        max_retries = 3
        for attempt in range(max_retries + 1):
            try:
                client = bigquery.Client()
                break
            except Exception as e:
                if attempt < max_retries:
                    logging.info(f"Error connecting to BigQuery, retrying... ({attempt + 1}/{max_retries})")
                else:
                    raise Exception("Could not connect to BigQuery after several attempts.") from e
                
        return client

    def create_table_if_not_exists(self, schema, table_name: str):
        #verificar si la tabla existe
        try:
            self.client.get_table(table_name)
            print(f"The table {table_name} alraedy exists.")
        except Exception:
            table = bigquery.Table(table_name, schema=schema)
            self.client.create_table(table)
            print(f"The table {table_name} has been created.")

    def upload_dataframe_to_bigquery(self, df: pd.DataFrame, table_name:str):
        job = self.client.load_table_from_dataframe(df, table_name)
        job.result()  # Esperar a que el job termine
        logging.info(f"Data uploaded to BigQuery in the table {table_name}.")



class Product:
    def __init__(self):
        config = read_json(here / "config.json")
        self.site_id = config["site_id"]

        # product infrmation
        self.product_name: str = config["product_name"]
        self.product_condition: str = config["product_condition"]
        self.items_atributes: str = config["items_attributes"]
        
        # To consult the currency conversion
        self.from_currency: str  = config["from_currency"]
        self.to_currency: str = config["to_currency"]

        # To consult the Meli data
        self.host: str = config["host"]

    def fetch_currency_conversion(self) -> dict | None:
        
        endpoint = f"/currency_conversions/search?from={self.from_currency}&to={self.to_currency}"

        url = f"{self.host}{endpoint}"

        print(url)

        response = requests.get(url, headers={"Authorization": os.getenv("MELI_API_KEY")})
        
        if response.status_code == 200:
            data = response.json()
            extracted_data = {
                "currency_base": data.get("currency_base"),
                "currency_quote": data.get("currency_quote"),
                "rate": data.get("rate")
            }
            return extracted_data
        else:
            logging.error(f"Error fetching data: {response.status_code}")
            return None

    def fetch_product(self) -> str | None:

        endpoint = f"/sites/{self.site_id}/search?q={self.product_name}&condition={self.product_condition}"

        url = f"{self.host}{endpoint}"

        response = requests.get(url, headers={"Authorization": os.getenv("MELI_API_KEY")})
        
        if response.status_code == 200:
            data = response.json()
            ids = ",".join([product["id"] for product in data["results"]])
            print(ids)
            return ids
        else:
            logging.error(f"Error fetching data: {response.status_code}")
            return None
        
    def fetch_item(self: str) -> dict | None:
        
        ids = self.fetch_product()

        endpoint = f"/items?ids={ids}&attributes={self.items_atributes}"
        url = f"{self.host}{endpoint}"
        
        response = requests.get(url, headers={"Authorization": os.getenv("MELI_API_KEY")})
        if response.status_code == 200:
            data = response.json()

            data = {
                "id": data.get("id"),
                "site_id": data.get("site_id"),
                "title": data.get("title"),
                "currency_id": data.get("currency_id"),
                "seller_id": data.get("seller_id"),
                "category_id": data.get("category_id"),
                "price": data.get("price"),
                "sale_terms": data.get("sale_terms"),
                "warranty": data.get("warranty"),
                "shipping": data.get("shipping")
            }

            return data


def main():
    """
    Main function to execute the data fetching and uploading process.
    Steps:
    1. Create an instance of the Product class.
    2. Fetch currency conversion data using the fetch_currency_conversion method.
    3. Fetch item data using the fetch_item method.
    4. Print the fetched currency conversion data.
    5. Print the fetched item data.
    6. Convert the fetched currency conversion data into a pandas DataFrame.
    7. Convert the fetched item data into a pandas DataFrame.
    8. Print the first 10 rows of the items DataFrame.
    9. Create an instance of the DataBase class.
    10. Construct the full table name for the currencies table.
    11. Construct the full table name for the items table.
    12. Create the currencies table in BigQuery if it does not exist using the create_table_if_not_exists method.
    13. Upload the currencies DataFrame to the currencies table in BigQuery using the upload_dataframe_to_bigquery method.
    14. Create the items table in BigQuery if it does not exist using the create_table_if_not_exists method.
    15. Upload the items DataFrame to the items table in BigQuery using the upload_dataframe_to_bigquery method.
    """

    pruduct = Product()
    currencies_data = pruduct.fetch_currency_conversion()
    items_data = pruduct.fetch_item()


    print(currencies_data)
    print(items_data)

    currencies_dataframe = pd.DataFrame([currencies_data])
    items_dataframe = pd.DataFrame([items_data])
    print(items_dataframe.head(10)) 
    
    database = DataBase()
    currency_table = f"{database.dataset_id}.{database.tables['currencies']}"
    item_table = f"{database.dataset_id}.{database.tables['items']}"


    database.create_table_if_not_exists(database.currency_schema, currency_table)
    database.upload_dataframe_to_bigquery(currencies_dataframe, currency_table)


    database.create_table_if_not_exists(database.item_schema, item_table)
    database.upload_dataframe_to_bigquery(items_dataframe, item_table)


if __name__ == "__main__":
    main()


