import pyodbc
from datetime import datetime

def convert_datetimeoffset_to_datetime(dto):
    return dto.replace(tzinfo=None)

# Connect to the database
conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=inx_platform;Uid=sa;Pwd=dellaBiella2!;TrustServerCertificate=yes;Connection Timeout=30;'
conn = pyodbc.connect(conn_str)
curs = conn.cursor()

# Get column information
sql_statement = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'inx_platform_members_user'"
curs.execute(sql_statement)
columns = curs.fetchall()

print(columns)

query='SELECT '
for column in columns:
    column_name, data_type = column.COLUMN_NAME, column.DATA_TYPE

    # Convert DATETIMEOFFSET columns to string
    if data_type == 'datetimeoffset':
        query += f"CONVERT(VARCHAR, {column_name}, 127) AS {column_name}, "
    else:
        query += f"{column_name}, "

# Remove the trailing comma and space
query = query.rstrip(", ")

# Add the FROM clause
query += f" FROM dbo.inx_platform_members_user"
print(query)

