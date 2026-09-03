import clickhouse_connect

def check():
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    databases = client.command('SHOW DATABASES')
    print("Databases in ClickHouse:", databases)
    try:
        tables = client.command('SHOW TABLES FROM lakehouse')
        print("Tables in lakehouse database:", tables)
    except Exception as e:
        print("Lakehouse db not yet initialized:", e)

if __name__ == '__main__':
    check()
