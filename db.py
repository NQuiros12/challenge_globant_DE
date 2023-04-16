import mysql.connector
import logging
from mysql.connector.pooling import MySQLConnectionPool

# Create a logger instance
logger = logging.getLogger(__name__)

# Configure the logger
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
file_handler = logging.FileHandler("log_db.txt")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class Connector(object):
    # Create a constructor
    def __init__(self, database: str, host: str, user: str, password: str, port: int, pool_size: int = 5):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.pool = MySQLConnectionPool(pool_size=pool_size, pool_name="mypool",
                                         host=self.host, database=self.database,
                                         user=self.user, password=self.password, port=self.port)

    # Get a connection from the pool
    def get_connection(self):
        try:
            conn = self.pool.get_connection()
            logger.info(f"Got connection from pool: {conn}")
            return conn
        except mysql.connector.Error as error:
            logger.error(f"Error getting connection from pool: {error}")
            raise

    # Execute a query in the database
    def execute(self, query):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            logger.info(f"Executed query: {query}")
            self.cursor = cursor
            self.conn = conn
            return cursor
        except mysql.connector.Error as error:
            logger.error(f"Error executing query: {query}. Error message: {error}")
            raise

    # Fetch the result but only return the first result
    def fetchone(self):
        result = self.cursor.fetchone()
        logger.debug(f"Fetched one result: {result}")
        return result

    # Fetch the result and return all the set.
    def fetchall(self):
        result = self.cursor.fetchall()
        logger.debug(f"Fetched all results: {result}")
        self.cursor.close()
        return result

    # Close the db connection.
    def close(self):
        self.conn.close()
        if not self.conn:
            logger.info("Closed successfully!")
