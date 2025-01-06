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

## Big query 
### Queries that answer the questions proposed in the challenge.

1. Query to answer if there is a seller with multiple listings. If so, how many?

```
SELECT
    seller_id,
    COUNT(title) AS cantidad_publicaciones
FROM
    `ml_challenge.items_details`
GROUP BY
    seller_id
ORDER BY
    cantidad_publicaciones DESC;
```

2. Query to get the average sales per seller.

```
SELECT 
  AVG(qty_sales) AS avg_sales_per_seller
FROM 
  `ml_challenge.sellers_details`
```

3. With this query, we can obtain the average price in dollars of the products. 

```
SELECT  
  ROUND(AVG(i.price * c.rate), 2) AS avg_sales_in_usd
FROM 
  `ml_challenge.items_details` i
JOIN 
  `ml_challenge.currency_conversions` c
ON 
  i.currency_id = c.currency_base
WHERE 
  c.currency_base = 'ARS' AND c.currency_quote = 'USD'
```

4.  To obtain the percentage of items with a warranty, the following query must be executed. This query will also return the percentage of items without a warranty and the total number of items on which the percentage is based.

```
SELECT 
    SUM(
        CASE WHEN warranty_type != 'Sin garantía' THEN 1 ELSE 0 END
        ) AS con_garantia,
    SUM(
        CASE WHEN warranty_type = 'Sin garantía' THEN 1 ELSE 0 END
        ) AS sin_garantia,
    COUNT(title) AS total_articulos,
    (SUM(CASE 
        WHEN 
        warranty_type != 'Sin garantía' THEN 1 ELSE 0 END) * 100.0 / COUNT(title)) AS porcentaje_con_garantia
FROM 
    `ml_challenge.items_details`
```

5. This query shows the different types of shipping that sellers offer for their products. We can also query the methods that are not offered.

```
SELECT 
  seller_id,
  CASE 
    WHEN free_shipping = TRUE THEN 'Offers free shipping'
    WHEN free_shipping = FALSE THEN 'Does not offer free shipping'
    ELSE 'No data on free shipping'
  END AS free_shipping_status,
  CASE 
    WHEN local_pick_up = TRUE THEN 'Offers local pick up'
    WHEN local_pick_up = FALSE THEN 'Does not offer local pick up'
    ELSE 'No data on local pick up'
  END AS local_pick_up_status,
  CASE 
    WHEN logistic_type IS NOT NULL THEN CONCAT('Logistic type: ', logistic_type)
    ELSE 'No logistic type data'
  END AS logistic_type_status,
  CASE 
    WHEN shipping_mode IS NOT NULL THEN CONCAT('Shipping mode: ', shipping_mode)
    ELSE 'No shipping mode data'
  END AS shipping_mode_status
FROM 
  `ml_challenge.items_details`
GROUP BY 
  seller_id, 
  free_shipping, 
  local_pick_up, 
  logistic_type, 
  shipping_mode
ORDER BY 
  seller_id;
  ```