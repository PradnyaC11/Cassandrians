import psycopg2
from datetime import datetime

def execute_sql(conn, sql_statement, params=None):
    with conn.cursor() as cursor:
        cursor.execute(sql_statement, params)
    conn.commit()

def prepare_transaction(conn, transaction_name):
    with conn.cursor() as cursor:
        cursor.execute(f"PREPARE TRANSACTION '{transaction_name}';")

def commit_prepared_transaction(conn, transaction_name):
    with conn.cursor() as cursor:
        # Close the initial transaction
        cursor.execute("COMMIT;")
        # Commit the prepared transaction
        cursor.execute(f"COMMIT PREPARED '{transaction_name}';")

def rollback_prepared_transaction(conn, transaction_name):
    # Ensure that we are not inside a transaction block before rolling back
    with conn.cursor() as cursor:
        cursor.execute("ROLLBACK;")
    # Rollback the prepared transaction
    with conn.cursor() as cursor:
        cursor.execute(f"ROLLBACK PREPARED '{transaction_name}';")

def read_team_id(conn, player_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT team_id FROM players WHERE player_id = %s;", (player_id,))
        result = cursor.fetchone()
        return result[0] if result else None

def distributed_transaction():
    try:
        # Generate a unique transaction name using a timestamp
        transaction_name = f'my_transaction_{datetime.now().strftime("%Y%m%d%H%M%S")}'

        # Connect to the first PostgreSQL database
        conn1 = psycopg2.connect(
            dbname="cassandrians",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )

        # Connect to the second PostgreSQL database
        conn2 = psycopg2.connect(
            dbname="cassandrians",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5433"
        )

        # Assume you have a 'players' table with 'player_id' and 'team_id' columns

        # Step 1: Begin distributed transaction
        prepare_transaction(conn1, transaction_name)
        prepare_transaction(conn2, transaction_name)

        # Step 2: Perform updates on both databases within the same distributed transaction
        try:
            # Update on the first database
            execute_sql(conn1, "UPDATE players SET team_id = 1 WHERE player_id = 1")

            # Simulate some processing time
            print("Simulating processing time...")
            import time
            time.sleep(5)

            # Update on the second database
            execute_sql(conn2, "UPDATE players SET team_id = 1 WHERE player_id = 1")

            # Commit the distributed transaction
            commit_prepared_transaction(conn1, transaction_name)
            commit_prepared_transaction(conn2, transaction_name)

            print("Transaction committed successfully.")

            # Step 3: Read from both databases after committing the transaction
            team_id_1 = read_team_id(conn1, 1)
            team_id_2 = read_team_id(conn2, 1)

            print(f"After commit, Team ID for Player 1 in Database 1: {team_id_1}")
            print(f"After commit, Team ID for Player 1 in Database 2: {team_id_2}")
        except Exception as e:
            # Rollback the distributed transaction in case of an error
            print(f"Error: {e}")
            rollback_prepared_transaction(conn1, transaction_name)
            rollback_prepared_transaction(conn2, transaction_name)
            print("Transaction rolled back.")
    finally:
        # Close database connections
        conn1.close()
        conn2.close()

if __name__ == "__main__":
    distributed_transaction()
