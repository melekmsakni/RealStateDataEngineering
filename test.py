from pyhive import presto

conn = presto.connect(host='localhost', port=5000, catalog='hive')
cursor = conn.cursor()
cursor.execute('SELECT * FROM some_table LIMIT 10')
print(cursor.fetchall())
