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
        logging.info("Succesfully created the virtual table")
    except connector.Error as e:
        logging.error(f"Problem with create the virtual table: {e}")
    finally:
        cnx.close()


def retrieve_general_info():
    cnx = set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_extract_info = """
            WITH Menu AS(
                Select mi.MenuItemID,mi.Price,mi.Name,m.CategoryName
                FROM MenuItem as mi INNER JOIN MenuCategory as m
                ON m.MenuCategoryID=mi.MenuCategoryID
            ),
            OrdersDetails AS(
                Select o.OrderDetailsID,o.Quantity,o.Order_OrderID,
                m.Price,m.Name,m.CategoryName
                FROM Menu as m INNER JOIN OrderDetails as o
                ON o.MenuItemID=m.MenuItemID
            
            ),
            OrdersINFO AS(
                Select o.OrderID,od.Quantity,od.Price,od.Name,od.CategoryName,
                o.CustomerID,o.TotalCost
                FROM OrdersDetails as od INNER JOIN `Order` as o
                ON o.OrderID=od.Order_OrderID
            )

            SELECT 
            c.CustomerID, c.FullName,
            o.OrderID, o.TotalCost,
            o.Name,o.CategoryName
            FROM OrdersINFO as o
            LEFT JOIN Customer as c ON o.CustomerID=c.CustomerID
        """
        cursor.execute(sql_extract_info)
        logging.info("Succesfully extracted general inforamtions")
        results = cursor.fetchall()
        logging.info("Results:")
        for result in results:
            customer_id, fullname, order_id, total_cost, name, category_name = result
            logging.info(f"Customer id: {customer_id}")
            logging.info(f"Fill name: {fullname}")
            logging.info(f"Order id: {order_id}")
            logging.info(f"Total cost: {total_cost}")
            logging.info(f"Category name: {category_name}")
            logging.info(f"Name: {name}")
        cursor.close()

    except connector.Error as e:
        logging.error(f"Problem with extracted general inforamtions: {e}")

    finally:
        cnx.close()


def retrieve_menu_info():
    cnx = set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_menu_info = """
            WITH Menu AS(
                Select mi.MenuItemID,mi.Price,mi.Name,m.CategoryName
                FROM MenuItem as mi INNER JOIN MenuCategory as m
                ON m.MenuCategoryID=mi.MenuCategoryID
            ),
            OrdersDetails AS(
                Select o.MenuItemID,o.OrderDetailsID,o.Quantity,o.Order_OrderID,
                m.Price,m.Name,m.CategoryName
                FROM Menu as m INNER JOIN OrderDetails as o
                ON o.MenuItemID=m.MenuItemID
            
            )

            Select Name, CategoryName FROM OrdersDetails
            WHERE MenuItemID=ANY
            (Select  MenuItemID FROM OrdersDetails GROUP BY MenuItemID
                HAVING COUNT(*)>2
            )

        """
        cursor.execute(sql_menu_info)
        logging.info("Succesfully extracted menu inforamtions")
        results = cursor.fetchall()
        logging.info("Results:")
        for result in results:
            name, category_name = result
            logging.info(f"Category name: {category_name}")
            logging.info(f"Name: {name}")
        cursor.close()

    except connector.Error as e:
        logging.error(f"Problem with extracted menu inforamtions: {e}")

    finally:
        cnx.close()

def max_quantity_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_get_max_quantity="""
            CREATE PROCEDURE IF NOT EXISTS GetMaxQuantity()
            BEGIN
                SELECT MAX(`Total Quantity in Order`) as `Max Quantity in Order`
                FROM (
                    Select SUM(Quantity) as `Total Quantity in Order`
                    FROM OrderDetails 
                    Group By Order_OrderID
                ) as TotalQuantity;
            END
        """
        cursor.execute(sql_get_max_quantity)
        cursor.callproc('GetMaxQuantity')
        results=next(cursor.stored_results())
        dataset=results.fetchall()[0][0]
        logging.info(f'Max quantity : {dataset}')
        
    except connector.Error as e:
        logging.error(f"Problem with creating GetMaxQuantity stored procedure: {e}")

    finally:
        cnx.close()
 
def cancel_order_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_get_order_detail = """
            CREATE PROCEDURE IF NOT EXISTS CancelOrder(IN orderID INT)
            BEGIN
                DELETE FROM `Order` WHERE OrderID=orderID;
            END
        """
        cursor.execute(sql_get_order_detail)
        cursor.callproc('CancelOrder',(5,))
        logging.info("Succesfully deleted row")
          
    except connector.Error as e:
        logging.error(f"Problem with creating Cancel Order stored procedure: {e}")

    finally:
        cnx.close()
        
def order_detail_parametrized_query():
    cnx=set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_get_order_detail = """
            SELECT OrderID, OrderDate, TotalCost 
            FROM `Order`
            WHERE OrderID = %s;
        """
        cursor.execute(sql_get_order_detail,(5,))
        result = cursor.fetchall()
        for row in result:
            order_id,order_date,total_cost=row
            logging.info(f"Order id: {order_id}")
            logging.info(f"Order date: {order_date}")
            logging.info(f"Total cost: {total_cost}")
          
    except connector.Error as e:
        logging.error(f"Problem with creating GetOrderDetail parametrized query: {e}")

    finally:
        cnx.close()
        
cancel_order_stored_procedure()