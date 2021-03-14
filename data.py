import os
import sqlite3
import sys

def sqlite_connection(db_name):
    new_table_query = '''CREATE TABLE IF NOT EXISTS document_data(
                        pk_id INTEGER PRIMARY KEY,
                        filename VARCHAR(50),
                        content BLOB);'''

    try:
        connection = sqlite3.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(new_table_query)
        connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Error creating a table", error)

    finally:
        return connection

def sqlite_insert(filename, connection):
    insert_query = '''INSERT INTO document_data (filename, content) VALUES (?, ?)'''
    with open(filename, 'rb') as f:
        data_tuple = (filename, f.read())
        cursor = connection.cursor()
        cursor.execute(insert_query, data_tuple)
        connection.commit()
        cursor.close()
        f.close()



def main(db_name, folder_path):
    connection = sqlite_connection(db_name)
    if not connection:
        print("Exiting")
        sys.exit()

    for root, dirs, files in os.walk(folder_path):
        for file_ in files:
            sqlite_insert(file_, connection)

    connection.close()



main('test', '.')
