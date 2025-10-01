import os
import logging
import psycopg2
from psycopg2 import pool

# Global connection pool variable
connection_pool = None

def initialize_pool():
    """
    Initializes the PostgreSQL connection pool using environment variables.
    """
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            20, # maxconn
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB")
        )
        logging.info("PostgreSQL connection pool initialized successfully.")
    except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
        logging.error(f"Error initializing PostgreSQL connection pool: {e}", exc_info=True)
        # Handle the error appropriately, maybe exit the application
        raise

def get_db_connection():
    """
    Gets a connection from the pool.
    """
    if connection_pool is None:
        logging.error("Connection pool is not initialized. Cannot get connection.")
        raise Exception("Connection pool not initialized.")
    return connection_pool.getconn()

def release_db_connection(conn):
    """
    Releases a connection back to the pool.
    """
    if connection_pool is None:
        logging.error("Connection pool is not initialized. Cannot release connection.")
        return
    connection_pool.putconn(conn)

def close_pool():
    """
    Closes all connections in the pool.
    """
    if connection_pool:
        connection_pool.closeall()
        logging.info("PostgreSQL connection pool closed.")

# Initialize the pool when the module is imported
initialize_pool()