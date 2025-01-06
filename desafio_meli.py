import os
import pandas as pd
import requests
import logging
from dotenv import load_dotenv
from pathlib import Path
from google.cloud import bigquery

from utils import get_items_data, get_sellers_data, read_json

load_dotenv()
MELI_API_KEY = os.getenv("MELI_API_KEY")

here = Path(__file__).parent

logging.basicConfig(level=logging.INFO)


class DataBase:

    def __init__(self):
        """
        Initializes the instance by reading the configuration file, creating a BigQuery client, and setting up dataset and table information.

        Attributes:
            client (bigquery.Client): The BigQuery client instance.
            dataset_id (str): The ID of the BigQuery dataset.
            tables (dict): A dictionary containing table names and their configurations.
            currency_schema (bigquery.SchemaField): The schema for the currency table.
            item_schema (bigquery.SchemaField): The schema for the item table.
        """
        config = read_json(here / "config.json")
        self.client = self._create_bigquery_client()
        self.dataset_id: str = config["database"]["dataset_id"]
        self.tables: dict = config["database"]["tables"]
        self.currency_schema = self.currency_schema()
        self.item_schema = self.item_schema()
        self.seller_schema = self.seller_schema()


    def _set_gcp_credentials(self):
        """
        Sets the Google Cloud Platform (GCP) credentials for the application.

        This method sets the environment variable `GOOGLE_APPLICATION_CREDENTIALS` 
        to the path of the GCP credentials JSON file, enabling the application to 
        authenticate with GCP services.

        Raises:
            FileNotFoundError: If the GCP credentials file does not exist at the specified path.
        """
        credentials_path = here / "gcp_credentials.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)


    def currency_schema(self) -> list[bigquery.SchemaField]:
        """
        Returns the schema for currency data.

        The schema defines the following fields:
        - currency_base: The base currency (STRING).
        - currency_quote: The quote currency (STRING).
        - rate: The exchange rate between the base and quote currencies (FLOAT).

        Returns:
            list[bigquery.SchemaField]: A list of schema fields for currency data.
        """
        return [
            bigquery.SchemaField("currency_base", "STRING"),
            bigquery.SchemaField("currency_quote", "STRING"),
            bigquery.SchemaField("rate", "FLOAT"), 
        ]


    def seller_schema(self) -> list[bigquery.SchemaField]:
        """
        Generates the schema for the seller table in BigQuery.

        Returns:
            list[bigquery.SchemaField]: A list of SchemaField objects defining the schema for the seller table.
                - id (INTEGER): The unique identifier for the seller.
                - qty_sales (INTEGER): The quantity of sales made by the seller.
        """
        return [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("qty_sales", "INTEGER"),
        ]

    def item_schema(self) -> list[bigquery.SchemaField]:
        """
        Returns the schema for an item in BigQuery.

        Returns:
            list[bigquery.SchemaField]: A list of BigQuery schema fields defining the structure of an item.
                - category_id (STRING): The category identifier.
                - price (INTEGER): The price of the item.
                - seller_id (INTEGER): The identifier of the seller.
                - title (STRING): The title of the item.
                - currency_id (STRING): The currency identifier.
                - free_shipping (BOOL): Indicates if the item has free shipping.
                - local_pick_up (BOOL): Indicates if local pick up is available.
                - logistic_type (STRING): The type of logistics used.
                - shipping_mode (STRING): The mode of shipping.
                - warranty_time (STRING): The duration of the warranty.
                - warranty_type (STRING): The type of warranty.
        """
        return [    
            bigquery.SchemaField("category_id", "STRING"),
            bigquery.SchemaField("price", "INTEGER"),
            bigquery.SchemaField("seller_id", "INTEGER"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("currency_id", "STRING"),
            bigquery.SchemaField("free_shipping", "BOOL"),
            bigquery.SchemaField("local_pick_up", "BOOL"),
            bigquery.SchemaField("logistic_type", "STRING"),
            bigquery.SchemaField("shipping_mode", "STRING"),
            bigquery.SchemaField("warranty_time", "STRING"),
            bigquery.SchemaField("warranty_type", "STRING")
        ]
    
    def _create_bigquery_client(self) -> bigquery.Client:
        """
        Creates a BigQuery client with retries.
        This method sets the Google Cloud Platform (GCP) credentials and attempts to create a BigQuery client.
        If the connection fails, it retries up to a maximum number of attempts.
        Returns:
            bigquery.Client: An instance of the BigQuery client.
        Raises:
            Exception: If the client could not be created after several attempts.
        """

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
        """
        Check if a BigQuery table exists, and create it if it does not.

        Args:
            schema (List[bigquery.SchemaField]): The schema of the table to be created.
            table_name (str): The name of the table to check or create.
        """
        try:
            self.client.get_table(table_name)
        except Exception:
            table = bigquery.Table(table_name, schema=schema)
            self.client.create_table(table)


    def upload_dataframe_to_bigquery(self, df: pd.DataFrame, table_name:str):
        """
        Uploads a pandas DataFrame to a specified BigQuery table.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the BigQuery table where the data will be uploaded.
        """
        job = self.client.load_table_from_dataframe(df, table_name)
        job.result() 
        logging.info(f"Data uploaded to BigQuery in the table {table_name}.")



class Item:
    def __init__(self):
        """
        Initializes the class with configuration data from a JSON file.
        Attributes:
            site_id (str): The site identifier.
            product_name (str): The name of the product.
            product_condition (str): The condition of the product.
            items_attributes (str): The attributes of the items.
            from_currency (str): The currency to convert from.
            to_currency (str): The currency to convert to.
            host (str): The host for consulting Meli data.
        """
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

        self.seller_attributes: str = config["seller_attributes"]

        self.items_data = self.fetch_data()

    def fetch_currency_conversion(self) -> dict | None:
        """
        Fetches the currency conversion rate from the specified base currency to the target currency.
        This method constructs the API endpoint URL using the base and target currencies, sends a GET request to the
        Mercado Libre API, and processes the response to extract the relevant currency conversion data.
        Returns:
            dict | None: A dictionary containing the extracted currency conversion data if the request is successful,
                         or None if the request fails.
        """
        
        endpoint = f"/currency_conversions/search?from={self.from_currency}&to={self.to_currency}"

        url = f"{self.host}{endpoint}"

        response = requests.get(url, headers={"Authorization": os.getenv("MELI_API_KEY")})
        if response.status_code != 200:
            logging.error(f"Error fetching data: {response.status_code}")
            return None

        data = response.json()
        extracted_data = {
            "currency_base": data.get("currency_base"),
            "currency_quote": data.get("currency_quote"),
            "rate": data.get("rate")
        }

        return extracted_data

    def fetch_data(self) -> dict | None:
        """
        Fetches data from the MercadoLibre API based on the site ID, product name, and product condition.
        Constructs the endpoint URL using the provided site ID, product name, and product condition,
        then sends a GET request to the MercadoLibre API.
        Returns:
            dict | None: The JSON response from the API as a dictionary if the request is successful (status code 200),
                 otherwise None.
        """

        endpoint = f"/sites/{self.site_id}/search?q={self.product_name}&condition={self.product_condition}"

        url = f"{self.host}{endpoint}"
        response = requests.get(url, headers={"Authorization": MELI_API_KEY})
        
        if response.status_code != 200:
            logging.error(f"Error fetching data: {response.status_code}")
            return None

        data = response.json()
        return data

    def get_items_ids(self) -> str:
        """
        Retrieves a comma-separated string of item IDs from the items_data attribute.

        Returns:
            str: A comma-separated string of item IDs.
        """
        ids = ",".join([product["id"] for product in self.items_data["results"]])
        return ids
    
    def get_sellers_ids(self) -> str:
        """
        Extracts and returns a comma-separated string of seller IDs from the items data.

        Returns:
            str: A comma-separated string of seller IDs.
        """
        ids = ",".join([str(product["seller"]["id"]) for product in self.items_data["results"]])
        return ids

    def chunk_ids(self, ids_str:str, chunk_size:int=20):
        """
        Splits a comma-separated string of IDs into chunks of a specified size.
        Args:
            ids_str (str): A comma-separated string of IDs.
            chunk_size (int, optional): The size of each chunk. Defaults to 20.
        Yields:
            list: A list containing a chunk of IDs.
        """
        ids_list = ids_str.split(',')
        
        for i in range(0, len(ids_list), chunk_size):
            yield ids_list[i:i + chunk_size]  

    def fetch_items(self) -> list | None:
        """
        Fetches items from the API based on product IDs.
        This method retrieves product IDs using the `fetch_product` method, chunks them into smaller
        groups, and fetches item data for each chunk from the API. The fetched item data is then
        processed and aggregated into a single list.
        Returns:
            list: A list of item data dictionaries if successful.
            None: If no IDs are found or if the API request fails.
        """
        all_items_data = []

        for id_chunk in self.chunk_ids(self.get_items_ids(), 20):  
            id_items = ",".join(id_chunk)  
            endpoint = f"/items?ids={id_items}&attributes={self.items_atributes}"
            
            url = f"{self.host}{endpoint}"

            response = requests.get(url, headers={"Authorization": MELI_API_KEY})

            if response.status_code != 200:
                return None
            
            data = response.json()

            items = get_items_data(data)

            all_items_data.extend(items)

        return all_items_data

    def fetch_seller(self) -> list | None:
        """
        Fetches seller data from the API.
        This method retrieves seller data by making requests to the API in chunks of 20 seller IDs at a time.
        It constructs the appropriate endpoint and handles the API response.
        Returns:
            list | None: A list of seller data if the request is successful, otherwise None.
        """

        all_sellers_data = []

        for id_chunk in self.chunk_ids(self.get_sellers_ids(), 20):  
            id_sellers = ",".join(id_chunk)
            endpoint = f"/users?ids={id_sellers}&attributes={self.seller_attributes}"
            
            url = f"{self.host}{endpoint}"

            response = requests.get(url, headers={"Authorization": MELI_API_KEY})

            if response.status_code != 200:
                return None
            
            data = response.json()

            sellers = get_sellers_data(data)

            all_sellers_data.extend(sellers)

        return all_sellers_data


def main():
    """
    Main function to execute the data fetching and uploading process.
    Steps:
    1. Create an instance of the Item class.
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

    item = Item()
    currencies_data = item.fetch_currency_conversion()
    items_data = item.fetch_items()
    sellers_data = item.fetch_seller()
    
    currencies_dataframe = pd.DataFrame([currencies_data])
    items_dataframe = pd.DataFrame(items_data)
    sellers_dataframe = pd.DataFrame(sellers_data)

    database = DataBase()
    currency_table = f"{database.dataset_id}.{database.tables['currencies']}"
    item_table = f"{database.dataset_id}.{database.tables['items']}"
    sellers_table = f"{database.dataset_id}.{database.tables['sellers']}"


    database.create_table_if_not_exists(database.currency_schema, currency_table)
    database.upload_dataframe_to_bigquery(currencies_dataframe, currency_table)


    database.create_table_if_not_exists(database.item_schema, item_table)
    database.upload_dataframe_to_bigquery(items_dataframe, item_table)

    database.create_table_if_not_exists(database.seller_schema, sellers_table)
    database.upload_dataframe_to_bigquery(sellers_dataframe, sellers_table)


if __name__ == "__main__":
    main()


