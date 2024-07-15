import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="@admin#2024*10",
    database="Minute_batches"
)
cursor = conn.cursor()

with open("ohlc_groups.sql", 'r') as table:
    mysql_table = table.read()

for statement in mysql_table.split(';'):
    statement = statement.strip()
    if statement:
        cursor.execute(statement)

conn.commit()  

# Close connections
cursor.close()
conn.close()
