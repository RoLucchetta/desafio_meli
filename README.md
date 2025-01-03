# MercadoLibre Data Fetching and Uploading Project

This project is designed to fetch product and currency conversion data from the MercadoLibre API and upload it to Google BigQuery for further analysis.

## Project Structure

- `.env`: Contains environment variables, including the MercadoLibre API key.
- `config.json`: Configuration file with details about the site, product, currency conversion, and BigQuery dataset and tables.
- `desafio_meli.py`: Main script to fetch data from the MercadoLibre API and upload it to BigQuery.
- `README.md`: Project description and instructions.

## Setup

1. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

2. Set up your environment variables in the .env file:
    ```env
    MELI_API_KEY="Bearer YOUR_MELI_API_KEY"
    ```

3. Update the config.json file with your specific configuration.

## Usage

Run the main script to fetch data from the MercadoLibre API and upload it to BigQuery:
```sh
python desafio_meli.py
```