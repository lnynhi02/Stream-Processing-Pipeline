import configparser
import psycopg2

config = configparser.ConfigParser()

config.read("config/config.ini")

host = config['database']['host']
port = config['database']['port']
user = config['database']['user']
password = config['database']['password']
dbname = config['database']['database']

# Create connection to the postgres database
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    print("Create connection sucessfully!")

except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

cur = conn.cursor()

def execute_sql(sql_query):
    try:
        cur.execute(sql_query)
        cur.execute(sql_query)
        conn.commit()
        print("Create table sucessfully")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()

def create_table():
    yellow_tripdata = '''
        CREATE TABLE IF NOT EXISTS yellow_tripdata(
            vendor_id TEXT,
            pickup_datetime TEXT,
            dropoff_datetime TEXT,
            passenger_count TEXT,
            trip_distance TEXT,   
            ratecode_id TEXT         
            pu_location_id TEXT,
            do_location_id TEXT,
            payment_type TEXT,
            fare_amount TEXT,
            extra TEXT,
            mta_tax TEXT,
            tip_amount TEXT,
            tolls_amount TEXT,
            improvement_surcharge TEXT,
            total_amount TEXT,
            congestion_surcharge TEXT,
            airport_fee TEXT
        )
    '''

    execute_sql(yellow_tripdata)
    cur.close()
    conn.close()


if __name__ == '__main__':
    create_table()