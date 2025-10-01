import os
import logging
from psycopg2 import pool

# The pool is initialized to None at the module level.
db_pool = None

def _initialize_pool():
    """
    Initializes the connection pool. This function is called lazily
    by get_db_connection the first time a connection is requested in a process.
    """
    global db_pool
    # Check again inside the function to handle race conditions if using threads.
    if db_pool is None:
        try:
            db_pool = pool.SimpleConnectionPool(
                1,  # minconn
                20, # maxconn
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host="db",
                port="5432",
                database=os.getenv("POSTGRES_DB")
            )
            logging.info("PostgreSQL connection pool initialized successfully in this process.")
        except Exception as e:
            logging.error(f"Failed to initialize PostgreSQL connection pool: {e}", exc_info=True)
            # Ensure db_pool remains None on failure to prevent further attempts.
            db_pool = None

def get_db_connection():
    """
    Gets a connection from the pool, initializing the pool if necessary.
    """
    if db_pool is None:
        _initialize_pool()

    if db_pool:
        return db_pool.getconn()
    return None

def release_db_connection(conn):
    """Releases a connection back to the pool."""
    if db_pool:
        db_pool.putconn(conn)