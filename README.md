# customer-shopping-behavior-analysis

--- 

## Project Background
In the modern retail industry, understanding customer purchasing behavior is essential to improving sales strategies, optimizing inventory planning, and strengthening customer loyalty. With many physical retailers experiencing declining sales due to the shift toward online shopping, data-driven insights have become increasingly valuable.
 
Using the Customer Shopping Behavior Dataset, this project aims to uncover trends related to customer demographics, preferred categories, spending patterns, payment methods, and seasonal purchasing behavior.

---

## Project Output

This project delivers the following components:

1. Automated ETL Pipeline (Apache Airflow)
    - Extracts data from PostgreSQL
    - Performs cleaning & transformation (Python/pandas)
    - Loads the processed data into ElasticSearch

2. Data Validation (Great Expectations)
    - 7 expectation rules were applied
    - All validation results: success = true

3. Kibana Dashboard
    - 8 interactive visualizations
    - Includes bar charts, pie charts, heatmaps, tag clouds, and trend analysis

4. Business Recommendation Report
    - Data-driven marketing and promotional strategy recommendations

---

## Dataset Information

Source: Customer Shopping Behavior Dataset (Kaggle)
Size: ~3,900 rows × 18 columns

#### Key Columns

- customer_id - Unique customer ID
- age - Customer age
- gender - Male/Female
- category - Purchased product category
- purchase_amount - Transaction amount (USD)
- season - Season at time of purchase
- payment_method - Payment preference
- previous_purchases - Shopping history

#### Data Characteristics

- No missing values or duplicates on raw import
- Normalized column names (snake_case, lowercase) after cleaning

## Technologies Used

- **Languages**: Python 3.9  
- **Databases**: PostgreSQL (Dockerized)  
- **Pipeline & Validation**: Apache Airflow, Great Expectations  
- **Search & Visualization**: ElasticSearch, Kibana  

**Python Libraries**  

- pandas - Data processing
- psycopg2 - PostgreSQL connection
- elasticsearch, helpers - Data loading
- great_expectations - Data validation

---

## References

- Kaggle - Customer Shopping Behavior Dataset (https://www.kaggle.com/datasets/rehan497/customer-shopping-behavior-dataset)

- Statista (2024) - Online shopper demographics 
(https://www.statista.com/topics/13741/online-shopping-behavior-worldwide/?srsltid=AfmBOoqE-iGj_2s7OWOibEBSkl-kEAyeSAwarErAeUSUQQidQ4JFVHlJ#topicOverview)

- Apache Airflow, Great Expectations, ElasticSearch, Kibana  

---
