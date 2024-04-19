import pyodbc
import os
import re
from datetime import datetime
import pytz
import shutil
import zipfile
import argparse
import sql_reserved_words

parser = argparse.ArgumentParser(description='Extract SQL Server database objects scripts')
group_1 = parser.add_argument_group('SQL Server arguments')
group_2 = parser.add_argument_group('Objects to create')
group_3 = parser.add_argument_group('Destination server')
group_1.add_argument('--server', type=str, required=True, help='SQL Server address')
group_1.add_argument('--database', type=str, required=True, help='Database name')
group_1.add_argument('--username', type=str, required=True, help='Database username')
group_1.add_argument('--password', type=str, required=True, help='Database password')

group_2.add_argument('--tables', action="store_true", required=False, help='toggle True/False to create table scripts')
group_2.add_argument('--data', action='store_true', required=False, help='toggle True/False to create table data scripts')
group_2.add_argument('--views', action='store_true', required=False, help='toggle True/False to create view scripts')
group_2.add_argument('--stored_procs', action='store_true', required=False, help='toggle True/False to create stored procedures scripts')

group_3.add_argument('--dest', type=str, required=False, help='Destination server, can be wither "local" or "inxeu" - Optional argument')

args = parser.parse_args()

server_name = args.server
database_name = args.database
username = args.username
password = args.password

output_directory = "./db_scripts/"

# Get terminal width in columns (char)
terminal_size = shutil.get_terminal_size()  # This contains heigth too (lines)
terminal_width = terminal_size.columns

# This is enabling that if the table is longer than batch_size_limit, we will make more INSERT files
batch_toggle = True     
batch_size = 999
spare_remainers = 0

toggle_make_table = args.tables
toggle_make_views = args.views
toggle_make_stored = args.stored_procs
toggle_make_data = args.data
destination_server = args.dest

counter = 1
view_counter = 1

generated_tables = []

newline = "\n"
tab="\t"

def generate_createtable_script(the_counter, the_table_name, the_schema, the_cursor, toggle_for_createdata):
    global counter
    global generated_tables
    # if toggle_for_createdata == 'True':
    #     toggle_for_createdata = True
    # else:
    #     toggle_for_createdata = False
    table_has_identity = False
    if f'{the_schema}.{the_table_name}' in generated_tables:
        print (the_table_name, 'already created, skipping ...')
        return
    put_section_title(f"Extracting Table definitions ... {the_table_name}")
    the_table_w_schema = the_schema + '.' + the_table_name
    # Get columns information for the table (name includes the schema)
    columns_query = f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, IS_NULLABLE, COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY, COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsComputed') AS IS_COMPUTED FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA+'.'+TABLE_NAME = '{the_table_w_schema}'"
    the_cursor.execute(columns_query)
    columns = cursor.fetchall()
    
    # Retrieve primary key columns
    primary_key_query = f"SELECT COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{the_table_name}'"
    cursor.execute(primary_key_query)
    primary_keys = cursor.fetchall()
    primary_key_columns = [key[0] for key in primary_keys]
    primary_key_columns_temp = []
    for primary_key_column in primary_key_columns:
        primary_key_columns_temp.append(normalize_string_name(primary_key_column))
        # if any(char in primary_key_column for char in (".", "-", " ", "/")):
        #     primary_key_columns_temp.append("[" + primary_key_column + "]")
        # else:
        #     primary_key_columns_temp.append(primary_key_column)
    primary_key_columns = primary_key_columns_temp
    print (the_table_name, "primary keys:", primary_key_columns)
    
    # Generate table creation script
    table_script = f"CREATE TABLE {the_table_w_schema} ({newline}"
    for column in columns:
        col_name, data_type, max_length, column_default, is_nullable, is_identity, is_computed = column
        col_name = normalize_string_name(col_name)
        # if any(char in col_name for char in (".", "-", " ", "/")):
        #     col_name = "[" + col_name + "]"
        # if col_name == 'Order' or col_name == 'order' or col_name == 'Date':
        #     col_name = "[" + col_name + "]"
        
        if data_type in ['nvarchar', 'nchar', 'varchar', 'char']:
            if max_length == -1:
                column_script = f"{col_name} {data_type}(MAX)"
            else:
                column_script = f"{col_name} {data_type}({max_length})"
        else:
            column_script = f"{col_name} {data_type}"
            
        if column_default:
            column_script += f" DEFAULT {column_default}"

        if not is_nullable or col_name in primary_key_columns:
            column_script += " NOT NULL"

        if is_identity:
            column_script += " IDENTITY(1,1)"
            table_has_identity = True
        
        if col_name in primary_key_columns and len(primary_keys) == 1:
            column_script += " PRIMARY KEY"

        if is_computed:
            column_script += " AS "  # Include the computed expression

        table_script += f"{tab}{column_script}, {newline}"
        
    # Add multi-field primary key if any
    if len(primary_keys) > 1:
        p_keys = ', '.join(f"{item[0]}" for item in primary_keys)
        table_script += f"{tab}CONSTRAINT {primary_keys[0][1]} PRIMARY KEY ({p_keys}),{newline}"
    
    # Retrieve foreign key relations
    relations_query = f"SELECT OBJECT_NAME(f.parent_object_id) AS table_name, COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name, OBJECT_NAME (f.referenced_object_id) AS referenced_table_name, OBJECT_SCHEMA_NAME(f.referenced_object_id) AS referenced_schema_name, COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name, f.name AS ForeignKeyName FROM sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id WHERE OBJECT_NAME(f.parent_object_id) = '{the_table_name}'"
    cursor.execute(relations_query)
    relations = cursor.fetchall()

    # Generate foreign key constraints in the table script
    if relations:
        print (f"Found {len(relations)} relationships in {the_table_name}")
        list_of_relations = []
        for relation in relations:
            _, column_name, referenced_table_name, referenced_schema_name, referenced_column_name, foreignkeyname = relation
            list_of_relations.append(referenced_table_name)
        for relation in relations:
            # there are foreign keys
            _, column_name, referenced_table_name, referenced_schema_name, referenced_column_name, foreignkeyname = relation
            print (terminal_width * f'*')
            print (f'{the_schema}.{the_table_name} relations:', list_of_relations)
            print (terminal_width * f'*')
            print (f'considering ... {referenced_schema_name}.{referenced_table_name}')
            if not referenced_schema_name + '.' + referenced_table_name in generated_tables:
                generate_createtable_script(the_counter, referenced_table_name, referenced_schema_name, the_cursor, toggle_for_createdata)
            fk_constraint_script = f"{tab}CONSTRAINT {normalize_string_name(foreignkeyname)} FOREIGN KEY ({normalize_string_name(column_name)}) REFERENCES {normalize_string_name(referenced_table_name)}({normalize_string_name(referenced_column_name)})"
            table_script += f"{fk_constraint_script},{newline}"
        else:
            print (f"Found no foreign keys in {referenced_table_name}")
    
    table_script = table_script.rstrip(f",{newline}")  # Remove trailing comma and space
    table_script += f"{newline});"
    
    if write_the_file(output_directory, f"table_{str(counter).zfill(4)}_{the_schema + '.' + the_table_name}.sql", table_script):
        generated_tables.append(the_schema + '.' + the_table_name)
    
    if toggle_for_createdata:
        generate_tabledata_script(cursor, the_schema, the_table_name, table_has_identity)
    
    counter = counter + 1

def normalize_string_name(string):
    original_case = string  # Store the original case
    string = string.upper()  # Convert to uppercase for comparison
    reserved_words = sql_reserved_words.reserved_words
    special_characters = [".", "-", " ", "/"]
    
    # if any(word in string for word in reserved_words) or any(char in string for char in special_characters):
    if string in reserved_words or any(char in string for char in special_characters):
        string = "[" + original_case + "]"  # Use the original case for the result
    else:
        string = original_case
    return string

def make_tables(curs, toggle_data):
    # Get schemas
    schema_query = "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_OWNER = 'dbo'"
    cursor.execute(schema_query)
    schemas = cursor.fetchall()
    schema_script = ""
    
    for schema in schemas:
        if schema[0] != 'dbo':
            # schema_script += f"CREATE SCHEMA {schema[0]};{newline}"
            schema_script = f"""
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema[0]}')
                BEGIN
                    EXEC('CREATE SCHEMA {schema[0]};');
                END;
                """
    write_the_file(output_directory, f"schemas.sql", schema_script)
    
    strings_for_exclusion = ['budget', 'backup', 'sysdiag', '2023', 'django_session']
    tables_query = f"SELECT TABLE_SCHEMA + N'.' + TABLE_NAME AS [Table] FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    for string_for_exclusion in strings_for_exclusion:
        tables_query += f" AND TABLE_NAME NOT LIKE '%{string_for_exclusion}%'"    
    curs.execute(tables_query)
    tables = cursor.fetchall()
    for table in tables:
        schema_name, table_name_only = table[0].split('.')
        print()
        # if table_name_only == 'KE24_import':
        generate_createtable_script(counter, table_name_only, schema_name, cursor, toggle_data)

def generate_tabledata_script(curs, schemaname, tablename, identity):
    
    global spare_remainers

    # Building SQL stetment and capturing DATETIMEOFFSET data type
    # Get column information
    sql_statement_for_column_types = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schemaname}' AND TABLE_NAME = '{tablename}'"
    curs.execute(sql_statement_for_column_types)
    columns = curs.fetchall()
    query='SELECT '
    for column in columns:
        column_name, data_type = column.COLUMN_NAME, column.DATA_TYPE
        # Convert DATETIMEOFFSET columns to string
        if data_type == 'datetimeoffset':
            query += f"CONVERT(VARCHAR, {normalize_string_name(column_name)}, 127) AS {normalize_string_name(column_name)}, "
        else:
            query += f"{normalize_string_name(column_name)}, "

    # Remove the trailing comma and space
    query = query.rstrip(", ")
    # Add the FROM clause
    query += f" FROM {schemaname}.{tablename}"

    # Retrieve data from the table
    data_query = f"SELECT * FROM {schemaname}.{tablename}"
    data_query = query
    print (f"Retreiving data from the table {schemaname}.{tablename} ")
    curs.execute(data_query)
    try:
        data = cursor.fetchall()
    except Exception as e:
        print(e)
        os.sys.exit('Program terminated')
    
    # get list of columns
    columns_query = f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA+'.'+TABLE_NAME = '{schemaname}.{tablename}' ORDER BY ORDINAL_POSITION"
    cursor.execute(columns_query)
    columns_result = cursor.fetchall()
    columns = [[col[0], col[1]] for col in columns_result]
    columns_len = len(columns)

    # Generate data insertion scripts if there are data
    if data:
        length_of_data = len(data)
        print (f"{schemaname}.{tablename} contains {length_of_data} rows")
        list_of_columns = []
        column_counter = 1
        there_is_a_datetime = False
        datetime_column_name = None
        # making a dictionary {(col_position: col_name), (col_position: col_name)}
        datetime_column_position = None
        datetime_dict ={}
        for column_name, dtype in columns:
            print (f"adding brackets to column names - column {column_counter}/{columns_len}", end="\r")
            # if any(char in column_name for char in (".", "-", " ", "/", "Order", "Date", "Time", "Version", "Year")):
            #     column_name = "[" + column_name + "]"
            column_name = normalize_string_name(column_name)
            if dtype == 'datetime':
                datetime_dict[column_counter -1] = dtype
            if dtype == 'time':
                datetime_dict[column_counter - 1] = dtype
            list_of_columns.append(column_name)
            column_counter += 1
        print() # This adds a new line in the terminal
        list_of_columns = ', '.join(list_of_columns).rstrip(",")

        insert_statement = f"INSERT INTO {schemaname}.{tablename} ({list_of_columns}) VALUES \n"  # Include column names in INSERT statement
        
        insert_script = insert_statement
        insert_script_perbatch = insert_statement
        
        batch_counter = 1       # counting rows in the batch
        batch_number = 1        # counting the number of batches
        row_counter = 1         # counting the number of table rows
        
        # Adding +1 if the table is smaller tha the batch_size (e.g. 0.45 -> 0 +1 = 1)
        how_many_batches = int(length_of_data / batch_size) + 1
        remaining_rows = length_of_data
        for row in data:
            row_values = table_row_treatment(row, toggle_datetime = there_is_a_datetime, datetime_col_name = datetime_column_name, datetime_col_position = datetime_column_position, dictionary_of_dt = datetime_dict)

            insert_script += f"({', '.join(row_values)}),\n"
            # --- per batch ---------
            insert_script_perbatch += f"({', '.join(row_values)}),\n"
            # --- per batch close ---
            if batch_counter == batch_size or batch_counter == remaining_rows:
                # Reached batch batch size limit or the end of the last smaller batch
                # Close the statement and add another INSER INTO in teh same file (non batched)
                insert_script = insert_script.rstrip(",\n")     # Remove trailing comma and new line
                insert_script += f";\n\n"                       # Add a semicolumn to terminate the statement
                # Add a new INSERT INTO statement, if the length_of_data is greater than batch_size
                if length_of_data > batch_size: insert_script += insert_statement
                batch_number += 1
                batch_counter = 0
                #--- per batch ---------------
                if batch_toggle and length_of_data > batch_size:
                    # Need to close the batch file and save it
                    insert_script_perbatch = insert_script_perbatch.rstrip(",\n")    # Remove trailing comma and new line
                    insert_script_perbatch += f";"                                   # Add a semicolumn to terminate the statement
                    filename = f"data_{str(counter).zfill(4)}_{schemaname}.{tablename}_batch_{str(batch_number-1).zfill(3)}_of_{str(how_many_batches).zfill(3)}.sql"
                    write_the_file(output_directory, filename, insert_script_perbatch)
                    print(f"Written {filename} ...", end ="\r")
                    insert_script_perbatch = insert_statement
                    remaining_rows -= batch_size
                # --- per batch close ---------
            batch_counter += 1
            if not batch_toggle: print (f"{schemaname}.{tablename} batch({batch_number}) - line", row_counter, "/", length_of_data, end="\r")
            os.sys.stdout.flush()
            row_counter += 1
        
        insert_script = insert_script.rstrip(",\n")     # Remove trailing comma and new line

        filename = f"data_{str(counter).zfill(4)}_{schemaname}.{tablename}.sql"
        if not batch_toggle or length_of_data < batch_size:
            write_the_file(output_directory, filename, insert_script)

def make_views(curs):
    global view_counter
    put_section_title("Extracting Views ...")
    view_scripts = {}
    curs.execute("SELECT name, definition FROM sys.objects o INNER JOIN sys.sql_modules m ON o.object_id = m.object_id WHERE o.type = 'V' AND o.name NOT LIKE '%database_firewall_rules%'")
    views = curs.fetchall()
    simple_list_of_views = [item[0] for item in views]
    for view in views:
        # Cycle through views and add them to view_scripts dictionary
        view_name = view.name
        view_definition = view.definition
        view_scripts[view_name] = {'dependencies': [], 'script': view_definition}
        # Find dependencies of the current view
        query_dependencies = f"SELECT referenced_entity_name FROM sys.dm_sql_referenced_entities('dbo.{view_name}', 'OBJECT')"
        cursor.execute(query_dependencies)
        dependencies = cursor.fetchall()
        # Loop through current view dependencies
        for dependency in dependencies:
            view_scripts[view_name]['dependencies'].append(dependency.referenced_entity_name)
        view_scripts[view_name]['dependencies'] = list(set(view_scripts[view_name]['dependencies'])) # Here I make a distinct
        # Qui devo controllare se ciascuno dei nomi è una view
        dependencies = view_scripts[view_name]['dependencies']
        dependency_only_views = []
        for dependency in dependencies:
            if dependency in simple_list_of_views:
                dependency_only_views.append(dependency)
            else:
                result = False
        view_scripts[view_name]['dependencies'] = dependency_only_views # Here I put in the list 'dependencies of the dictionary 
    script_order = []
    visited_views = set()
    for view_name in view_scripts:
        build_view_script(view_name, visited_views, script_order, view_scripts)

def put_section_title(title):
    section_title = title
    section_title_length = len(section_title)
    print ("*" * terminal_width)
    print ("*", section_title , " " * (terminal_width - section_title_length - 6), "*")
    print ("*" * terminal_width)

def build_view_script(view_name, visited, order, view_scripts):
    global view_counter
    if view_name in visited:
        return

    dependencies = view_scripts[view_name]['dependencies']
    for dependency in dependencies:
        build_view_script(dependency, visited, order, view_scripts)
    
    # Make the script
    script = view_scripts[view_name]['script']
    filename = f"views_{str(view_counter).zfill(4)}_{view_name}.sql"
    print (filename)
    if write_the_file(output_directory, filename, script):
        view_counter += 1
    visited.add(view_name)

def make_stored(curs):
    # Generate stored procedure scripts
    procedures_query = "SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'"
    curs.execute(procedures_query)
    procedures = cursor.fetchall()
    counter = 1
    put_section_title("Extracting stored procedures ...")
    for procedure in procedures:
        procedure_name, procedure_script = procedure
        # Here we get the script
        query = f"EXEC sp_helptext '{procedure_name}'"
        cursor.execute(query)
        script = ""
        for row in cursor.fetchall():
            script += row[0]
        # Save the procedure script to a file
        filename = f"stored_{str(counter).zfill(4)}_{procedure_name}.sql"        
        if write_the_file(output_directory, filename, script):
            counter += 1
            print(filename)

def write_the_file (output_path, file_name, file_content):
    script_path = os.path.join(output_path, file_name)

    if not os.path.exists(output_path):
        try:
            os.makedirs(output_path)
        except Exception as e:
            print(f"Failed to create folder: {output_path}")
            print(str(e))
            return False

    try:
        with open(script_path, "w") as file:
            file.write(file_content)
            return True
    except Exception as e:
        print (f"An error occurred")
        print (str(e))
        return False

def table_row_treatment(the_row, **kwargs):
    dict = kwargs['dictionary_of_dt']
    for col, dtype in dict.items():
        if dtype == 'datetime' and not the_row[col] == None :
            the_row[col] = datetime(the_row[col].year, the_row[col].month, the_row[col].day, the_row[col].hour, the_row[col].minute, the_row[col].second)
        if dtype == 'time':
            the_row[col] = str(the_row[col])

    row_values = ["'" + str(value.replace("'", "''") ) + "'" if isinstance(value, str) else value for value in the_row]
    # row_values = [str(element).replace("[EU]", "[[EU]]") for element in row_values]
    row_values = [str(element).replace("[", "[[") for element in row_values]
    row_values = [str(element).replace("]", "]]") for element in row_values]
    row_values = ["-1" if element == "True" else element for element in row_values]
    row_values = ["0" if element == "False" else element for element in row_values]
    row_values = ["NULL" if element == "None" else element for element in row_values]
    # check date with a regular expression in teh row_values list
    row_values = ["'" + str(element) + "'" if re.match(r"\d{4}-\d{2}-\d{2}", str(element)) else element for element in row_values]

    return row_values

def clean_output_folder(folder):
    if os.path.exists(folder):
        file_list = os.listdir(folder)
        for file_name in file_list:
            file_path = os.path.join(folder, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        # the folder does not exist, make it
        os.mkdir(folder)

def zip_output_folder(folder, server, db):
    zip_filename = server + "_" + db + "_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".zip"
    print (zip_filename)
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
        # Walk through the folder and add all its files and subdirectories to the zip file
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder)  # Get the relative path
                zipf.write(file_path)
        return zip_filename

if __name__ == '__main__':
    # Delete all files in the output directory
    # if it does not exist, let's make it
    clean_output_folder(output_directory)

    # Create a connection string
    conn_str = f"Driver={{ODBC Driver 18 for SQL Server}};Server={server_name};Database={database_name};Uid={username};Pwd={password};TrustServerCertificate=yes;Connection Timeout=30;"

    # Connect to the SQL Server database
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print (e)
        os.sys.exit("Connection to DB failed")
    
    if toggle_make_table:
        make_tables(cursor, toggle_make_data)
    if toggle_make_views:
        make_views(cursor)
    if toggle_make_stored:
        make_stored(cursor)
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

    zip_file = zip_output_folder(output_directory, server_name, database_name)

    #Review syntax of destination push
    if destination_server:
        os.system(f'python push.py {destination_server} {database_name} {zip_file}')