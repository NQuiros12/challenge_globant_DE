# README

This is my solution for the data engineer evaluation at Globant.

I created this API using MySQL as the database and FastAPI with Python as the programming language. It provides methods for loading a CSV file and batch loading it into the database. Additionally, it has one endpoint for each query in the second section, as required. I also added some additional CRUD methods and endpoints to the API.

As for the bonus part, I have implemented some tests in the "test" directory. The available tests are:

* Test connection to the database
* Test the upload_csv function for the database
* Test the batch loading for the database
* Test for duplicate data in the hired employees table
* Test for rows in the tables
* Test that the queries requested in section 2 of the   evaluation are returning a code 200, which means everything is working fine. I don't think it's necessary or optimal to view the query results themselves, as they will be tied to the contents of the table and its changes over time.
