import logging
import os

import mysql.connector as connector
from connection import set_up_connection

def check_booking_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        sql_use_db="""
            USE littlelemondb
        """
        cursor.execute(sql_use_db)
        sql_drop_procedure="""
            DROP PROCEDURE IF EXISTS CheckBooking;
        """
        cursor.execute(sql_drop_procedure)
        sql_check_booking_stored_procedure="""
            CREATE PROCEDURE CheckBooking(IN tableNO INT)
            BEGIN
                SELECT 
                    CASE 
                        WHEN tableNO in (Select TableNo FROM Booking) THEN CONCAT('Table ',tableNO,' is already booked')
                        ELSE CONCAT('Table ',tableNO,' is not already booked')
                    END as BookingStatus;
            END
        """
        cursor.execute(sql_check_booking_stored_procedure)
        cursor.callproc('CheckBooking',(3,))
        results=next(cursor.stored_results())
        dataset=results.fetchall()[0][0]
        print(dataset)
        logging.info(f"{dataset}")
    except connector.Error as e:
        logging.error(f"Problem with creating CheckBooking stored procedure: {e}")
    finally:
        cnx.close()
        
        
def add_valid_booking_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        cursor.execute("USE littlelemondb")
        sql_drop_procedure = """
            DROP PROCEDURE IF EXISTS CheckValidBooking;
        """
        cursor.execute(sql_drop_procedure)
        sql_add_valid_booking = """
            CREATE PROCEDURE CheckValidBooking(IN bookingDate DATE, IN tableNO INT)
            BEGIN
                DECLARE validBooking BOOLEAN;
                START TRANSACTION;
                SELECT 
                    COUNT(*) INTO validBooking
                FROM Booking
                WHERE BookingDate = bookingDate AND TableNo = tableNO;
                
                IF validBooking = 0 THEN
                    INSERT INTO Booking (BookingID, BookingDate, TableNo)
                    VALUES ((SELECT IFNULL(MAX(BookingID), 0) + 1 FROM Booking), bookingDate, tableNO);
                    SELECT CONCAT('Table ',tableNO,' on',bookingDate,' is not already booked')
                    COMMIT;
                ELSE
                    SELECT CONCAT('Table ',tableNO,' on ',bookingDate,' is already booked')
                    ROLLBACK;
                END IF;
            END;
        """
        cursor.execute(sql_add_valid_booking)
        cursor.callproc('CheckValidBooking',('2022-01-24',6))
        results=next(cursor.stored_results())
        dataset=results.fetchall()[0][0]
        logging.info(f"{dataset}")
    except connector.Error as e:
        logging.error(f"Problem with creating AddValidBooking stored procedure: {e}")
    finally:
        cnx.close()
        
        
def add_booking_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        cursor.execute("USE littlelemondb")
        cursor.execute("DROP PROCEDURE IF EXISTS AddBooking")
        sql_add_booking_stored_procedure="""
            CREATE PROCEDURE AddBooking(IN bookingDate DATE, IN tableNO INT)
            BEGIN
                DECLARE newBookingID INT;
                SELECT IFNULL(MAX(BookingID), 0) + 1 INTO newBookingID FROM Booking;
                INSERT INTO Booking (BookingID, BookingDate, TableNo)
                VALUES (newBookingID, bookingDate, tableNO);
                SELECT CONCAT('Table ',tableNO,' on',bookingDate,' is added to Booking');
            END;
        """
        cursor.execute(sql_add_booking_stored_procedure)
        cursor.callproc('AddBooking',('2022-11-24',4))
        results=next(cursor.stored_results())
        dataset=results.fetchall()[0][0]
        logging.info(f"{dataset}")
    except connector.Error as e:
        logging.error(f"Problem with creating AddBooking stored procedure: {e}")
    finally:
        cnx.close()
        
def update_booking_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        cursor.execute("USE littlelemondb")
        cursor.execute("DROP PROCEDURE IF EXISTS UpdateBooking")
        sql_update_booking_stored_procedure="""
            CREATE PROCEDURE UpdateBooking(IN bookingID INT, IN bookingDate DATE)
            BEGIN
                UPDATE Booking
                SET BookingDate=bookingDate
                WHERE BookingID=bookingID;
            END;
        """
        cursor.execute(sql_update_booking_stored_procedure)
        cursor.callproc('UpdateBooking',(1,'2022-11-24'))
        logging.info(f"Updated Booking")
    except connector.Error as e:
        logging.error(f"Problem with creating UpdateBooking stored procedure: {e}")
    finally:
        cnx.close()
        
        
        
def cancel_booking_stored_procedure():
    cnx=set_up_connection()
    try:
        cursor=cnx.cursor()
        cursor.execute("USE littlelemondb")
        cursor.execute("DROP PROCEDURE IF EXISTS CancelBooking")
        sql_cancel_booking_stored_procedure="""
            CREATE PROCEDURE CancelBooking(IN bookingID INT)
            BEGIN
                Delete FROM Booking
                WHERE BookingID=bookingID;
            END;
        """
        cursor.execute(sql_cancel_booking_stored_procedure)
        cursor.callproc('CancelBooking',(1,))
        logging.info(f"Cancel Booking")
    except connector.Error as e:
        logging.error(f"Problem with creating CancelBooking stored procedure: {e}")
    finally:
        cnx.close()