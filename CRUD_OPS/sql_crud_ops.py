'''
@Author: Girish
@Date: 2024-08-23
@Last Modified by: Girish
@Last Modified: 2024-08-23
@Title: CRUD operations to create and manage a database table using Python and SQL Server.
'''

import pyodbc

class Database:
    
    def __init__(self, driver, server, trusted_connection):
        '''
        Function: 
                  Initializes the Database object with the driver, server, and trusted connection details.

        Parameters:
             driver (str): The ODBC driver to use for the connection.
             server (str): The server name to connect to.
             trusted_connection (str): Indicates whether to use a trusted connection.

        Returns:
               None
        '''
        self.driver = driver
        self.server = server
        self.trusted_connection = trusted_connection
        self.conn = None

    def connect_to_server(self):
        '''
        Function: 
                  Establishes a connection to the SQL Server using the provided driver and server.

        Parameters:
             None

        Returns:
               conn (pyodbc.Connection): The connection object to the SQL Server.
        '''
        conn_str = f"DRIVER={self.driver};SERVER={self.server};TRUSTED_CONNECTION={self.trusted_connection}"
        self.conn = self._connect(conn_str)
        return self.conn

    def connect_to_db(self, db_name):
        '''
        Function: 
                  Establishes a connection to the specified database on the SQL Server.

        Parameters:
             db_name (str): The name of the database to connect to.

        Returns:
               conn (pyodbc.Connection): The connection object to the specified database.
        '''
        conn_str_with_db = f"DRIVER={self.driver};SERVER={self.server};DATABASE={db_name};TRUSTED_CONNECTION={self.trusted_connection}"
        self.conn = self._connect(conn_str_with_db)
        return self.conn

    def _connect(self, conn_str):
        '''
        Function: 
                  Connects to the SQL Server using the provided connection string.

        Parameters:
             conn_str (str): The connection string for the SQL Server.

        Returns:
               conn (pyodbc.Connection): The connection object if successful, otherwise None.
        '''
        try:
            conn = pyodbc.connect(conn_str, autocommit=True)
            print('Connected successfully.')
            return conn
        except pyodbc.Error as ex:
            print(f'Error connecting: {ex}')
            return None

    def create_database(self, db_name):
        '''
        Function: 
                  Creates a new database with the specified name.

        Parameters:
             db_name (str): The name of the database to create.

        Returns:
               success (bool): True if the database was created successfully, otherwise False.
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'CREATE DATABASE {db_name}')
            print(f'Database {db_name} created successfully!')
            return True
        except pyodbc.Error as ex:
            print(f'Error creating database: {ex}')
            return False

    def check_table_exists(self, table_name):
        '''
        Function: 
                  Checks if the specified table exists in the connected database.

        Parameters:
             table_name (str): The name of the table to check for existence.

        Returns:
               exists (bool): True if the table exists, otherwise False.
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
            if cursor.fetchone():
                return True
            else:
                print(f"Table '{table_name}' does not exist.")
                return False
        except pyodbc.Error as ex:
            print(f"Error checking table existence: {ex}")
            return False

    def create_table(self, table_name, columns):
        '''
        Function: 
                  Creates a new table with the specified columns in the connected database.

        Parameters:
             table_name (str): The name of the table to create.
             columns (list): A list of tuples where each tuple contains a column name and data type.

        Returns:
               None
        '''
        column_definitions = ", ".join([f"{column_name} {data_type}" for column_name, data_type in columns])
        query = f"CREATE TABLE {table_name} ({column_definitions})"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            print(f"Table {table_name} created successfully.")
        except pyodbc.Error as ex:
            print(f"Error creating table: {ex}")

    def insert_data(self, table_name):
        '''
        Function: 
                  Inserts data into the specified table in the connected database.

        Parameters:
             table_name (str): The name of the table where data will be inserted.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")  
                columns = [column[0] for column in cursor.description]
                num_rows = int(input("Enter the number of rows to insert: "))

                for _ in range(num_rows):
                    values = []
                    for column in columns:
                        while True:
                            try:
                                value = input(f"Enter value for {column}: ")
                                data_type = self._get_column_data_type(cursor, column)
                                self._validate_value(value, data_type)
                                values.append(value)
                                break
                            except ValueError as ve:
                                print(ve)
                            except pyodbc.Error as ex:
                                print(f"Invalid data type for column {column}. Please try again.")
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in values])})"
                    cursor.execute(query, values)
                print("Data inserted successfully.")
            except pyodbc.Error as ex:
                print(f"Error inserting data: {ex}")

    def _get_column_data_type(self, cursor, column_name):
        '''
        Function: 
                  Retrieves the data type of the specified column in the connected table.

        Parameters:
             cursor (pyodbc.Cursor): The cursor object for executing SQL queries.
             column_name (str): The name of the column to retrieve the data type for.

        Returns:
               data_type (str): The data type of the specified column.
        '''
        cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{cursor.tables().fetchall()[0][2]}' AND COLUMN_NAME = '{column_name}'")
        return cursor.fetchone()[0]

    def _validate_value(self, value, data_type):
        '''
        Function: 
                  Validates the input value against the expected data type.

        Parameters:
             value (str): The input value to validate.
             data_type (str): The expected data type for the value.

        Returns:
               None
        '''
        if data_type.startswith("int"):
            if not value.isdigit():
                raise ValueError(f"Invalid value '{value}' for integer column.")
        elif data_type.startswith("varchar"):
            return
        elif data_type.startswith("float"):
            try:
                float(value)
            except ValueError:
                raise ValueError(f"Invalid value '{value}' for float column.")

    def update_data(self, table_name, column_name, new_value, condition):
        '''
        Function: 
                  Updates data in the specified table based on a condition.

        Parameters:
             table_name (str): The name of the table to update.
             column_name (str): The name of the column to update.
             new_value (str): The new value to set for the column.
             condition (str): The condition to identify the rows to update.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            query = f"UPDATE {table_name} SET {column_name} = ? WHERE {condition}"
            try:
                cursor = self.conn.cursor()
                cursor.execute(query, (new_value,))
                print("Data updated successfully.")
            except pyodbc.Error as ex:
                print(f"Error updating data: {ex}")

    def delete_row(self, table_name, condition):
        '''
        Function: 
                  Deletes a row from the specified table based on a condition.

        Parameters:
             table_name (str): The name of the table to delete a row from.
             condition (str): The condition to identify the row to delete.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            query = f"DELETE FROM {table_name} WHERE {condition}"
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                print("Row deleted successfully.")
            except pyodbc.Error as ex:
                print(f"Error deleting row: {ex}")

    def delete_column(self, table_name, column_name):
        '''
        Function: 
                  Deletes a column from the specified table.

        Parameters:
             table_name (str): The name of the table to delete a column from.
             column_name (str): The name of the column to delete.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            query = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                print(f"Column {column_name} deleted successfully.")
            except pyodbc.Error as ex:
                print(f"Error deleting column: {ex}")

    def delete_table(self, table_name):
        '''
        Function: 
                  Deletes the specified table from the database.

        Parameters:
             table_name (str): The name of the table to delete.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            query = f"DROP TABLE {table_name}"
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                print(f"Table {table_name} deleted successfully.")
            except pyodbc.Error as ex:
                print(f"Error deleting table: {ex}")

    def add_column(self, table_name, column_name, data_type):
        '''
        Function: 
                  Adds a new column to the specified table.

        Parameters:
             table_name (str): The name of the table to add a column to.
             column_name (str): The name of the column to add.
             data_type (str): The data type of the new column.

        Returns:
               None
        '''
        if self.check_table_exists(table_name):
            query = f"ALTER TABLE {table_name} ADD {column_name} {data_type}"
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                print(f"Column {column_name} added successfully.")
            except pyodbc.Error as ex:
                print(f"Error adding column: {ex}")


def main():

    db = Database(driver='{ODBC Driver 17 for SQL Server}', server='GIRISHNEKAR\\sqlexpress', trusted_connection='yes')

    if not db.connect_to_server():
        print("Could not connect to server.")
        return

    while True:
        print("\nOptions:")
        print("1. Create a new database")
        print("2. Use an existing database")
        print("3. Exit")
        option = input("Enter your choice: ")

        if option == '1':
            db_name = input("Enter the name of the database to create: ")
            if db.create_database(db_name):
                if db.connect_to_db(db_name):
                    handle_table_operations(db)

        elif option == '2':
            db_to_connect = input("Enter the name of the database to connect to: ")
            if db.connect_to_db(db_to_connect):
                handle_table_operations(db)
            else:
                print(f"Cannot connect to database '{db_to_connect}'. Please check the database name and try again.")

        elif option == '3':
            print("Exiting the program.")
            break

        else:
            print("Invalid option. Please choose again.")

def handle_table_operations(db):
    '''
    Function: 
              Handles operations related to tables in the connected database.

    Parameters:
         db (Database): The Database object to perform operations on.

    Returns:
         None
    '''
    while True:
        print("\nTable Operations:")
        print("1. Create a new table")
        print("2. Insert data into an existing table")
        print("3. Update the table")
        print("4. Delete a row")
        print("5. Delete a column")
        print("6. Delete the table")
        print("7. Add a new column")
        print("8. Return to the main menu")
        t_option = input('Enter the option: ')

        if t_option == '1':
            table_name, columns = get_table_details()
            db.create_table(table_name, columns)

        elif t_option == '2':
            table_name = input("Enter the name of the table to insert data into: ")
            db.insert_data(table_name)

        elif t_option == '3':
            table_name = input("Enter the name of the table to update: ")
            column_name = input("Enter the column name to update: ")
            new_value = input("Enter the new value: ")
            condition = input("Enter the condition (e.g., id = 1): ")
            db.update_data(table_name, column_name, new_value, condition)

        elif t_option == '4':
            table_name = input("Enter the name of the table to delete a row from: ")
            condition = input("Enter the condition to delete (e.g., id = 1): ")
            db.delete_row(table_name, condition)

        elif t_option == '5':
            table_name = input("Enter the name of the table to delete a column from: ")
            column_name = input("Enter the column name to delete: ")
            db.delete_column(table_name, column_name)

        elif t_option == '6':
            table_name = input("Enter the name of the table to delete: ")
            db.delete_table(table_name)

        elif t_option == '7':
            table_name = input("Enter the name of the table to add a new column to: ")
            column_name = input("Enter the column name to add: ")
            data_type = input("Enter the data type of the new column: ")
            db.add_column(table_name, column_name, data_type)

        elif t_option == '8':
            return

        else:
            print("Invalid option. Please choose again.")

def get_table_details():
    '''
    Function: 
              Collects details for creating a new table including table name and columns.

    Parameters:
         None

    Returns:
         table_name (str): The name of the table.
         columns (list): A list of tuples containing column names and data types.
    '''
    table_name = input("Enter the table name: ")
    columns = []
    while True:
        column_name = input("Enter column name: ")
        data_type = input("Enter data type (e.g., INT, VARCHAR(50)): ")
        columns.append((column_name, data_type))
        another = input("Add another column? (y/n): ")
        if another.lower() != 'y':
            break
    return table_name, columns

if __name__ == "__main__":
    main()
