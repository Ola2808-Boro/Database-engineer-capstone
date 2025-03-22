import logging
import os

import mysql.connector as connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename="logs.log", format="%(asctime)s %(message)s", filemode="w")


def set_up_connection() -> connector.MySQLConnection:
    try:
        cnx = connector.connect(
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
        )
        logging.error("Succesfully set up the connection")
    except connector.Error as e:
        logging.error("Problem with set up the connection")
    return cnx


set_up_connection()
