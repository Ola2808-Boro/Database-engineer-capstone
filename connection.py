import logging
import os

import mysql.connector as connector
from dotenv import load_dotenv

load_dotenv()


def set_up_connection() -> connector.MySQLConnection:
    try:
        cnx = connector.connect(
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
        )
        logging.error("Succesfully set up the connection")
    except connector.Error as e:
        logging.error(f"Problem with set up the connection: {e}")
    return cnx

