import pyodbc
import shutil
import os
import re
import sys
import zipfile
import argparse

parser = argparse.ArgumentParser(description='Push database in SQL server')
group_1 = parser.add_argument_group('Strategy')
group_2 = parser.add_argument_group('Parameters')
group_3 = parser.add_argument_group('Data structure and data')

group_1.add_argument('--strat', type=str, required=True, help='strategy of creation: "drop_restore" or "empty_db"')

group_2.add_argument('--db_server', type=str, required=True, help='destination server name/address')
group_2.add_argument('--database', type=str, required=False, help='name of the database in teh destination server')
group_2.add_argument('--username', type=str, required=False, help='username of the database in the destination server')
group_2.add_argument('--password', type=str, required=False, help='password of the database in the destination server')

group_3.add_argument('--zip', type=str, required=True, help='file containing structure and data')

args = parser.parse_args()

server= args.db_server
database = args.database
username = args.username
password = args.password
zip_file_name = args.zip

extract_folder = "./"

drop_all_sp_name = 'DropAllTablesViewsProcedures'
drop_all_sp_script =f"""
    CREATE PROCEDURE [{drop_all_sp_name}]
    AS
    BEGIN
        SET NOCOUNT ON;

        -- Drop foreign keys
        DECLARE @ForeignKeyName NVARCHAR(128)
        DECLARE @ParentTableSchema NVARCHAR(128)
        DECLARE @ParentTableName NVARCHAR(128)
        DECLARE @ReferencedTableObjectID INT

        DECLARE foreign_key_cursor CURSOR FOR
            SELECT f.name, OBJECT_SCHEMA_NAME(f.parent_object_id) AS ParentSchema, OBJECT_NAME(f.parent_object_id) AS ParentTable, f.referenced_object_id
            FROM sys.foreign_keys f

        OPEN foreign_key_cursor

        FETCH NEXT FROM foreign_key_cursor INTO @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ReferencedTableObjectID
        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT('ForeignKeyName: ' + @ForeignKeyName)
            PRINT('ParentTableSchema: ' + @ParentTableSchema)
            PRINT('ParentTableName: ' + @ParentTableName)
            PRINT('ReferencedTableObjectID: ' + CONVERT(NVARCHAR, @ReferencedTableObjectID))

            DECLARE @DropForeignKeyStatement NVARCHAR(255)
            SET @DropForeignKeyStatement = 'ALTER TABLE ' + QUOTENAME(@ParentTableSchema) + '.' + QUOTENAME(@ParentTableName) + ' DROP CONSTRAINT ' + @ForeignKeyName
            PRINT('@DropForeignKeyStatement: ' + @DropForeignKeyStatement)
            EXEC sp_executesql @DropForeignKeyStatement

            FETCH NEXT FROM foreign_key_cursor INTO @ForeignKeyName, @ParentTableSchema, @ParentTableName, @ReferencedTableObjectID
        END

        CLOSE foreign_key_cursor
        DEALLOCATE foreign_key_cursor

        -- Drop tables
        DECLARE @TableName NVARCHAR(128)
        DECLARE @TableSchema NVARCHAR(128)
        DECLARE table_cursor CURSOR FOR
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'

        OPEN table_cursor
        FETCH NEXT FROM table_cursor INTO @TableSchema, @TableName

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @SqlStatement NVARCHAR(500)
            SET @SqlStatement = 'DROP TABLE ' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@TableName)
            EXEC sp_executesql @SqlStatement

            FETCH NEXT FROM table_cursor INTO @TableSchema, @TableName
        END

        CLOSE table_cursor
        DEALLOCATE table_cursor

        -- Drop views
        DECLARE @ViewName NVARCHAR(128)
        DECLARE view_cursor CURSOR FOR
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'

        OPEN view_cursor
        FETCH NEXT FROM view_cursor INTO @TableSchema, @ViewName

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @DropViewStatement NVARCHAR(255)
            SET @DropViewStatement = 'DROP VIEW ' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@ViewName)
            PRINT('@DropViewStatement: ' + @DropViewStatement)
            EXEC sp_executesql @DropViewStatement

            FETCH NEXT FROM view_cursor INTO @TableSchema, @ViewName
        END

        CLOSE view_cursor
        DEALLOCATE view_cursor

        -- Drop stored procedures
        DECLARE @ProcedureName NVARCHAR(128)
        DECLARE procedure_cursor CURSOR FOR
            SELECT routine_schema, routine_name
            FROM information_schema.routines
            WHERE routine_type = 'PROCEDURE'

        OPEN procedure_cursor
        FETCH NEXT FROM procedure_cursor INTO @TableSchema, @ProcedureName

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @DropProcedureStatement NVARCHAR(255)
            SET @DropProcedureStatement = 'DROP PROCEDURE ' + QUOTENAME(@TableSchema) + '.' + QUOTENAME(@ProcedureName)
            PRINT('@DropProcedureStatement: ' + @DropProcedureStatement)
            EXEC sp_executesql @DropProcedureStatement

            FETCH NEXT FROM procedure_cursor INTO @TableSchema, @ProcedureName
        END

        CLOSE procedure_cursor
        DEALLOCATE procedure_cursor

    END
"""

destination_database_name = database
last_table = ""

# Categories and their corresponding prefixes
categories = {
    'schema': 'schema',
    'table': 'table',
    'data': 'data',
    'view': 'view',
    'stored': 'stored'
}

identity_string = "NOT NULL IDENTITY("

# Get terminal width in columns (char)
terminal_size = shutil.get_terminal_size()  # This contains heigth too (lines)
terminal_width = terminal_size.columns

# We operate in the ./db_script folder, if it does not exist, it will be created
# If the folder exists, it will be removed with all its content and recreated
if os.path.exists(extract_folder + "db_scripts"):
    shutil.rmtree(extract_folder + "db_scripts")
with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
    # Extract all contents of the ZIP file to the specified directory
    zip_ref.extractall(extract_folder)

# Establish connection to the SQL Server
try:
    if server == 'localhost':
        connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database={database};Uid={username};Pwd={password};TrustServerCertificate=yes;Connection Timeout=10;'
        connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database=master;Uid={username};Pwd={password};TrustServerCertificate=yes;Connection Timeout=10;'
    else:
        connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database={database};Uid={username};Pwd={password};Connection Timeout=10;"
    conn = pyodbc.connect(connection_string, autocommit=True)
    cursor = conn.cursor()
    if cursor:
        print(f"connected to {server}")
except Exception as e:
    print(e)
    sys.exit()

# Setting SINGLE_USER, and kick off all other connections
# Remember to set back MULTI_USER later with ALTER DATABASE [{destination_database_name}] SET MULTI_USER;
# try:
#     cursor.execute(
#     f"""
#     USE master;
#     ALTER DATABASE [{destination_database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
#     """
#     )
#     cursor.commit()
#     print(f'Set SINGLE_USER on database {destination_database_name}')
# except Exception as e:
#     print ("Set single user failed\n",str(e))
#     os.sys.exit()



# What is the startegy: drop or empty
match args.strat:
    case "drop_restore":
        try:
            print(f"Dropping database {destination_database_name} ...", end="")
            cursor.execute(f'DROP DATABASE IF EXISTS {destination_database_name};')
            print("done")
        except Exception as e:
            print(f"dropped database {destination_database_name}")
            print (str(e))
            os.sys.exit()
        try:
            print (f"creating database {destination_database_name} ...", end="")
            cursor.execute(f'CREATE DATABASE {destination_database_name};')
            cursor.commit()
            print("done")
            cursor.execute(f"USE {destination_database_name}")
            cursor.commit()
            print(f"{destination_database_name} database in use now")
        except:
            print(f"creation of database {destination_database_name} failed")
            print (str(e))
            os.sys.exit()
    case "empty":
        try:
            # conn = pyodbc.connect(f"Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database={destination_database_name};Uid={username};Pwd={password};Connection Timeout=10;", autocommit=True)
            # cursor = conn.cursor()
            # q = """SELECT routine_name
            #     FROM information_schema.routines
            #     WHERE routine_type = 'PROCEDURE'
            #     """
            # print(f"server: {server}")
            # print(f"destination database: {destination_database_name}")
            # cursor.execute(q)
            # Fetch all rows
            # rows = cursor.fetchall()
            # Print the list of stored procedures
            # for row in rows:
            #     print(row.routine_name)
            # If the sp exists drop it
            # cursor.execute(f"IF OBJECT_ID('dbo.{drop_all_sp_name}', 'P') IS NOT NULL DROP PROCEDURE dbo.{drop_all_sp_name}")

            cursor.execute(f"USE {destination_database_name}")
            cursor.execute(f"IF OBJECT_ID('dbo.{drop_all_sp_name}', 'P') IS NOT NULL DROP PROCEDURE dbo.{drop_all_sp_name}")
            print(f"{drop_all_sp_name} dropped if existed")
            # Inject the sp
            cursor.execute(f"USE {destination_database_name}")
            cursor.execute(drop_all_sp_script)
            cursor.commit()
            print(f"{drop_all_sp_name} recreated")
            # Execute the sp
            cursor.execute(f"USE {destination_database_name}")
            print("prima")
            cursor.execute(f"EXEC dbo.{drop_all_sp_name}")
            print("mezzo")
            cursor.commit()
            print("dopo")
            print("drop_all_sp executed")

        except Exception as e:
            print(e)
            os.sys.exit()
    case _:
        print ("strategy not valid")
        os.sys.exit()

# Start the process of pushing data to the database
# Loop through categories and execute SQL scripts
for category, prefix in categories.items():
    message = f'Category:{category}'
    print(message, (terminal_width - len(message) - 2) * '-')
    folder_path = "./db_scripts"
    file_names = [file for file in os.listdir(folder_path) if file.startswith(prefix)]
    file_names.sort()
    length_of_last_message = 0
    last_message = ""
    for file_name in file_names:
        identity_present = False
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r') as file:
            sql_script = file.read()
            if category == 'data':
                # Get table name
                pattern = r"INSERT INTO (\w+)\.(\w+)"
                match = re.search(pattern, sql_script)
                if match:
                    schemaname = str(match.group(1))
                    tablename = str(match.group(2))
                if tablename != last_table: print()
                # Query database to see if the table has identity columns
                q = f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
                    AND TABLE_SCHEMA='{schemaname}' AND TABLE_NAME='{tablename}'
                """
                cursor.execute(q)
                identities = cursor.fetchall()
                if identities:
                    identity_present = True
                    sql_command = f"SET IDENTITY_INSERT {schemaname}.{tablename} ON;"
                    cursor.execute(sql_command)
                    cursor.commit()
                else:
                    if tablename != last_table:
                        print(f"\ntable {schemaname}.{tablename} does not have identities", end="\r")
            try:
                if sql_script == '': continue
                cursor.execute(sql_script)
                conn.commit()
                console_message = f"Executed script {file_name} in category {category}."
                length_of_message = len(console_message)
                print(length_of_last_message * " ", end="\r")
                print(console_message, end="\r")
                length_of_last_message = length_of_message
                last_message = console_message
                if category == 'data' and identity_present:
                    sql_command = f"SET IDENTITY_INSERT {schemaname}.{tablename} OFF;"
                    identity_present = False
                    cursor.execute(sql_command)
                    conn.commit()
                
            except Exception as e:
                print(f"Error executing script {file_name} in category {category}: {str(e)}")
                os.sys.exit()
        if category == 'data':
            last_table = tablename
    print()

# set database user mode to multi user
try:
    cursor.execute(
    f"""
    USE master;
    ALTER DATABASE [{destination_database_name}] SET MULTI_USER;
    """
    )
    cursor.commit()
    print(f'Set MULTI_USER on database {destination_database_name}')
except Exception as e:
    print ("Set multi user failed\n",str(e))
    os.sys.exit()
# Closing everything
cursor.close()
conn.close()
sys.exit('terminated')
