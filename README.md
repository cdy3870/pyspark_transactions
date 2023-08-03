# An Analysis of 100M+ Transactions and 10M+ Transactional Messages

This repo contains a big data analysis project where I primarily used PySpark to handle the millions of rows. Insights such as the most popular brands, products, and purchases throughout the month are visualized using Plotly. Transactional messages were also analyzed to see what type of marketing is effective and ineffective to hopefully aid in the success of online shopping sites.

## Requirements

This dashboard is hosted on Streamlit's community cloud and does not require you to run the code locally. You can access the site through this link: https://bigdatatransactions.streamlit.app/.

## Libraries, Frameworks, APIs, Cloud Services
1. Libraries and Frameworks
- Pyspark
- Pandas
- Streamlit
- Plotly
2. Data Source
- eCommerce Dataset (Kaggle)

## Some Key Analysis Performed and Insights
1. Transactions
- A Sankey diagram is used to look at the breakdown of product types, where electronics were clearly the most popular type of product with a majority of revenue coming from smartphones
- The middle of the month proved to have the most purchases and most amount of revenue generated
- Using a query to compute the average view to purchase and view to cart time of products showed that people were the least hesitant with buying Apple products
2. Transactional Messages
- By checking the subject lines of marketing emails, we can see that adding personalization, discounts, and deadlines often led to people checking the email. Suprisingly saleout indications did not lead to much of an increase in conversion.
- Trigger messages (such as abandoned cart) often led to people blocking the sender and transactional messages (such as notification of shipment) led to least adverse effects
