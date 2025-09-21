import sqlite3

conn = sqlite3.connect("nhahang.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM Nhân_sự")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()
