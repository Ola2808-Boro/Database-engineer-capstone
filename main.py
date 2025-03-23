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
        logging.error(f"Problem with set up the connection: {e}")
    return cnx


def create_virtual_table():
    cnx = set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_virtual_table = """
            CREATE VIEW OrdersView AS
            SELECT o.OrderID, o.TotalCost, SUM(od.Quantity) AS TotalQuantity
            FROM OrderDetails as od
            INNER JOIN `Order` as o
            ON o.OrderID = od.Order_OrderID
            GROUP BY o.OrderID, o.TotalCost;
        """
        cursor.execute(sql_virtual_table)
        cnx.commit()
        cursor.close()
        logging.error("Succesfully created the virtual table")
    except connector.Error as e:
        logging.error(f"Problem with create the virtual table: {e}")
    cnx.close()


create_virtual_table()
