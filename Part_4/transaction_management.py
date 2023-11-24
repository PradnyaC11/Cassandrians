import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time
# Function to execute a SQL statement
def execute_sql(conn, sql_statement, params=None):
    with conn.cursor() as cursor:
        cursor.execute(sql_statement, params)
    conn.commit()

# Function to execute a SQL statement and fetch the result
def execute_sql_and_fetch(conn, sql_statement, params=None):
    with conn.cursor() as cursor:
        cursor.execute(sql_statement, params)
        result = cursor.fetchall()
    return result

# Function to read team ID for a player
def read_team_id(player_id, thread_id):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="cassandrians",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    with conn.cursor() as cursor:
        sql_statement = "SELECT team_id FROM players WHERE player_id = %s;"
        params = (player_id,)
        result = execute_sql_and_fetch(conn, sql_statement, params)
        print(f"Thread {thread_id}: Reads team ID without transaction management for Player {player_id}: {result[0][0]}")
        print(f"Thread {thread_id}: without transaction management going to sleep to imply some processing")
        time.sleep(2)
        result = execute_sql_and_fetch(conn, sql_statement, params)
        print(f"Thread {thread_id}: Reads team ID without transaction management after sleep for Player {player_id}: {result[0][0]}")
    conn.close()

# Function to read team ID for a player
def read_team_id_trans(player_id, thread_id):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="cassandrians",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    try:

        with conn.cursor() as cursor:
            # cursor.execute("PREPARE TRANSACTION 'read_transaction';")
            cursor.execute("BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            sql_statement = "SELECT team_id FROM players WHERE player_id = %s;"
            params = (player_id,)
            result = execute_sql_and_fetch(conn, sql_statement, params)
            print(f"Thread {thread_id}: Reads team ID with transaction management for Player {player_id}: {result[0][0]}")
            print(f"Thread {thread_id}: with transaction management going to sleep to imply some processing")
            time.sleep(2)
            result = execute_sql_and_fetch(conn, sql_statement, params)
            print(f"Thread {thread_id}: Reads team ID with transaction management after sleep for Player {player_id}: {result[0][0]}")
            cursor.execute("COMMIT;")
            # cursor.execute("COMMIT PREPARED 'read_transaction';")
    except psycopg2.Error as e:
        print(f"Thread {thread_id}: Error in transaction: {e}")
    conn.close()
        

# Function to update team ID for a player
def update_team_id_no_transaction(player_id, new_team_id, thread_id):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="cassandrians",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            sql_statement = "UPDATE players SET team_id = %s WHERE player_id = %s;"
            params = (new_team_id, player_id)
            
            execute_sql(conn, sql_statement, params)
            cursor.execute("COMMIT;")
            print(f"Concurrently Thread {thread_id}: Updates team ID for Player {player_id} to {new_team_id}")


    except psycopg2.Error as e:
        conn.rollback()
        print(f"Thread {thread_id}: Error in transaction: {e}")
    conn.close()


# Player ID for testing
player_id = 1

# Simulate read and update operations concurrently
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(read_team_id, player_id, 1)
    executor.submit(update_team_id_no_transaction, player_id, 2, 2)

time.sleep(2)
print("Now lets see the same scenario with transaction management")
# Simulate read and update operations concurrently
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(read_team_id_trans, player_id, 1)
    executor.submit(update_team_id_no_transaction, player_id, 3, 2)
