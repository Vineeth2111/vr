Big Data Final Project – E-Commerce Analytics using Cassandra and Python

This project demonstrates a full Big Data pipeline using Apache Cassandra (Astra DB) and Python.
The goal was to process and analyze a large e-commerce dataset using the Bronze, Silver, and Gold architecture, and visualize key business insights.

Project Files
	•	connection.py – Connects to the Cassandra database using a secure bundle
	•	bronze_ingest.py – Loads raw data into the Bronze table
	•	silver_cleaning_cql.py – Filters and cleans the data, storing it in the Silver table
	•	gold_aggregation_cql.py – Aggregates total sales by country
	•	gold_top_products.py – Calculates top-selling products by revenue
	•	visualization.py – Plots sales by country (bar, pie, horizontal)
	•	visualization_top_products.py – Plots top-selling products


Bronze → Silver → Gold Flow

Bronze Layer
Raw e-commerce data is inserted into the bronze_raw_orders table.

Silver Layer
Data is cleaned to remove:
	•	Quantity less than or equal to 0
	•	UnitPrice less than or equal to 0
	•	Missing CustomerID

Gold Layer
Aggregated summaries:
	•	By country – total sales value
	•	By product – top-selling items

 Visualizations
	1.	Bar chart – Total sales by country
	2.	Pie chart – Top 5 countries by sales
	3.	Horizontal bar – Country-wise sales
	4.	Bonus: Bar chart – Top 10 best-selling products

 How to Run
	1.	Run the connection script to connect to Astra DB
	2.	Execute each step in this order:
    - python3 connection.py  
    - python3 bronze_ingest.py  
    - python3 silver_cleaning_cql.py  
    - python3 gold_aggregation_cql.py  
    - python3 gold_top_products.py  
    - python3 visualization.py  
    - python3 visualization_top_products.py  

My Reflection

I learned a lot about Big Data systems, specifically how Cassandra manages large sets of data.
This project exposed me to hands-on experience with building an end-to-end pipeline — from raw data ingestion to cleaning, aggregation, and visualization.

Notes
• The CSV file used in this project contains over 500,000 rows and was too large to upload to GitHub.
• Secure connect and token files were omitted from GitHub for security.
• The project scripts are ready and can be run locally if the dataset is available locally.
