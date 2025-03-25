import mysql.connector as connector
from connection import set_up_connection
import logging


def insert_data_to_staff_table():
    staff_data=[
        (1,'Manager','6500.00'),
        (2,'Waiter','4500.00'),
        (3,'Cook','6000.00'),
        (4,'Cook','6000.00'),
        (5,'Waiter','5500.00'),
    ]
    sql_insert="""
        INSERT IGNORE INTO Staff(StaffID,Role,Salary)
        VALUES(%s,%s,%s)
    """
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        cursor.executemany(sql_insert,staff_data)
        cnx.commit()
        cursor.close()
        logging.info("Succesfully inserted staff data")
    except connector.Error as e:
        logging.error(f"Problem with insert staff data: {e}")
    finally:
        cnx.close()
        
def insert_data_to_order_delivery_table():
    order_delivery_data=[
        (1,'in preparation','2022-10-10'),
        (2,'ready','2022-10-10'),
        (3,'ready','2022-10-10'),
        (4,'in preparation','2022-10-10'),

    ]
    sql_insert="""
        INSERT IGNORE INTO OrderDelivery(OrderDeliveryID,OrderDeliveryStatus,OrderDeliveryDate)
        VALUES(%s,%s,%s)
    """
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        cursor.executemany(sql_insert,order_delivery_data)
        cnx.commit()
        cursor.close()
        logging.info("Succesfully inserted rder_deliver data")
    except connector.Error as e:
        logging.error(f"Problem with insert order_delivery data: {e}")
    finally:
        cnx.close()
        
def insert_data_to_customer_table():
    customer_data=[
        (1,'Harry Potter','789-456-321'),
        (2,'Hermione Granger','345-764-212'),
        (3,'Draco Malfoy','234-634-735'),
        (4,'Ron Weasley','123-444-666'),

    ]
    sql_insert="""
        INSERT IGNORE INTO Customer(CustomerID,FullName,Phone)
        VALUES(%s,%s,%s)
    """
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        cursor.executemany(sql_insert,customer_data)
        cnx.commit()
        cursor.close()
        logging.info("Succesfully inserted customer data")
    except connector.Error as e:
        logging.error(f"Problem with insert customer data: {e}")
    finally:
        cnx.close()
    
          
def insert_data_to_order_table():
    data=[
        (1,'2022-10-10',654.32,1,1,2,1),
        (2,'2022-11-12',654.32,2,2,1,3),
        (3,'2022-10-11',654.32,3,3,1,4),
        (4,'2022-10-13',654.32,4,4,2,2),
    ]
    cnx=set_up_connection()
    try:
        cursor = cnx.cursor()
        sql_use_db = """
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        sql_insert="""
            INSERT IGNORE INTO `Order` (OrderID,OrderDate,TotalCost,OrderDeliveryID,CustomerID,StaffID,BookingID)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.executemany(sql_insert,data)
        cnx.commit()
        cursor.close()
        logging.info("Succesfully inserted order data")
          
    except connector.Error as e:
        logging.error(f"Problem with insert order data: {e}")

    finally:
        cnx.close()
    
    
def insert_data_to_booking_table():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db="""
            USE littlelemondb;
        """
        cursor.execute(sql_use_db)
        booking_data=[
            (1,'2022-01-22',6),
            (2,'2022-03-22',2),
            (3,'2022-02-21',3),
            (4,'2022-01-22',4)
        ]
        sql_insert="""
            INSERT IGNORE INTO Booking(BookingID,BookingDate,TableNo) 
            VALUES (%s,%s,%s);
        """
        cursor.executemany(sql_insert,booking_data)
        cnx.commit()
        cursor.close()
        logging.info("Succesfully inserted booking data")
    except connector.Error as e:
        logging.error(f"Problem with insert booking data: {e}")
    finally:
        cnx.close()
        
        
def insert_data_to_menu_category():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db="""
            USE littlelemondb
        """
        data_menu_category=[
            (1,'dessert'),
            (2,'drinks'),
            (3,'starter'),
            (4,'course')
        ]
        cursor.execute(sql_use_db)
        sql_insert="""
            INSERT IGNORE INTO MenuCategory(MenuCategoryID,CategoryName)
            VALUES(%s,%s)
        """
        cursor.executemany(sql_insert,data_menu_category)
        cnx.commit()
        cursor.close()
    except connector.Error as e:
        logging.error(f"Problem with insert menu category data: {e}")
    finally:
        cnx.close()
        
def insert_data_to_menu_item():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db="""
            USE littlelemondb
        """
        data_menu=[
            (1,32.45,'Apple pie',1),
            (2,35.45,'Black tea',2),
            (3,25.45,'Grilled vegetables',3),
            (4,55.45,'Roasted salmon',4)
        ]
        cursor.execute(sql_use_db)
        sql_insert="""
            INSERT IGNORE INTO MenuItem(MenuItemID,Price,Name,MenuCategoryID)
            VALUES (%s,%s,%s,%s)
        """
        cursor.executemany(sql_insert,data_menu)
        cnx.commit()
        cursor.close()
    except connector.Error as e:
        logging.error(f"Problem with insert menu item data: {e}")
    finally:
        cnx.close()
        
        
def insert_data_to_order_details():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db="""
            USE littlelemondb
        """
        cursor.execute(sql_use_db)
        data_order_details=[
            (1,1,2,2),
            (2,3,4,1),
            (3,2,3,4),
            (4,6,2,3)
        ]
        sql_insert="""
            INSERT IGNORE INTO OrderDetails(OrderDetailsID,Quantity,Order_OrderID,MenuItemID) 
            VALUES(%s,%s,%s,%s)
        """
        cursor.executemany(sql_insert,data_order_details)
        cnx.commit()
        cursor.close()
    except connector.Error as e:
        logging.error(f"Problem with insert order_details data: {e}")
    finally:
        cnx.close()
        
def insert_all_data():
    insert_data_to_booking_table()
    insert_data_to_customer_table()
    insert_data_to_menu_category()
    insert_data_to_menu_item()
    insert_data_to_staff_table()
    insert_data_to_order_delivery_table()
    insert_data_to_order_details()
    insert_data_to_order_table()