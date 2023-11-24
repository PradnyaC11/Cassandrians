import psycopg2
import random
#pylint:skip-file

DATABASE_NAME = 'cassandrians'
DB_USER = "postgres"
DB_PASS = "Happyplace11*"
DB_HOST = "localhost"
DB_PORT = 5432
REGIONS = ['India','England','Australia']
RUNS = [1,2,3,4,6]
BOWL_NO = [1,2,3,4,5,6]

def connect_potsgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    try:
        connection = psycopg2.connect(database=dbname,
                                      user=DB_USER,
                                      password=DB_PASS,
                                      host=DB_HOST,
                                      port=DB_PORT)
        print("Connection established successfully for database " + dbname)
        return connection
    except:
        print("Error occurred while connecting to the database")

def list_partition(curs):
    create_partition_table= "CREATE TABLE IF NOT EXISTS match_region (venue_id serial , country text) PARTITION BY LIST (country);"

    curs.execute(create_partition_table)
    conn.commit()

    partition_queries = [
                "CREATE TABLE IF NOT EXISTS INDIA PARTITION OF match_region FOR VALUES IN ('India');",
                "CREATE TABLE IF NOT EXISTS ENGLAND PARTITION OF match_region FOR VALUES IN ('England');",
                "CREATE TABLE IF NOT EXISTS AUSTRALIA PARTITION OF match_region FOR VALUES IN ('Australia');"
            ]
    
    for query in partition_queries:
        curs.execute(query)
        conn.commit()

def insert_list_data(curs):
    curs = conn.cursor()

    for i in range(1, 51):
        # Generate random data
        random_id = i
        region = random.choice(REGIONS)

        insert_query = f"INSERT INTO match_region (venue_id , country) VALUES ({random_id}, '{region}');"
        curs.execute(insert_query)

    conn.commit()

def range_partition(curs):

    table = f"CREATE TABLE IF NOT EXISTS runs_per_ball (batsman_id int, bowl_no int, runs_by_batter int) partition by range(runs_by_batter);"
    curs.execute(table)
    conn.commit()

    curs.execute(f"CREATE TABLE IF NOT EXISTS runs_below_3 PARTITION OF runs_per_ball FOR VALUES FROM ('1') TO ('4'); ")
    conn.commit()

    curs.execute(f"CREATE TABLE IF NOT EXISTS runs_boundaries PARTITION OF runs_per_ball FOR VALUES FROM ('4') TO ('7'); ")
    conn.commit()

def insert_range_data(curs):
    curs = conn.cursor()
    for i in range(1, 51):

        pid = i
        runs = random.choice(RUNS)
        bowl = random.choice(BOWL_NO)
        

        # Insert data into the sales table
        insert_query = f"INSERT INTO runs_per_ball (batsman_id, bowl_no , runs_by_batter) VALUES (%s, %s, %s);"
        values = (pid, bowl, runs)
        curs.execute(insert_query,values)

        conn.commit()
        
def start_containers():
    subprocess.run(["docker-compose", "-f", "docker-compose.yaml", "up", "-d"])

def stop_containers():
    subprocess.run(["docker-compose", "-f", "docker-compose.yaml", "down"])

def wait_for_replication():
    time.sleep(10)  

def rep():
    try:
        start_containers()
        wait_for_replication()
        print("PostgreSQL master-slave replication set up successfully!")
    except Exception as e:
        print(f"Error setting up master-slave replication: {e}")
    finally:
        stop_containers()

if __name__ == '__main__':

    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        list_partition(curs)
        insert_list_data(curs)
        range_partition(curs)
        insert_range_data(curs)
        rep()