from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv

cass1 = 'localhost:9042'
cass2 = 'localhost:9043'
cass3 = 'localhost:9044'

# Cassandra's authentication credentials
username = 'cassandra'
password = 'cassandra'

contact_points = ['localhost']

keyspace_name = 'commentary_keyspace'

# Name of the table in Cassandra
table_name = 'commentary'


def create_table():
    # Create a new table commentary
    table_query = ("CREATE TABLE IF NOT EXISTS commentary (" +
                   "Over_No TEXT," +
                   "Over_Score TEXT," +
                   "Short_comm TEXT," +
                   "Commentary TEXT," +
                   "Bold_Comm TEXT," +
                   "Innings_ID TEXT," +
                   "Ball_ID TEXT," +
                   "Match_ID TEXT," +
                   "PRIMARY KEY (Innings_ID, Ball_ID)" +
                   ");")

    session.execute(table_query)

    print("Commentary table created.\n")


def insert_csv_file_data():
    # Insert data from sample csv file to commentary table of cassandra node
    csv_file_path = 'ipl2019_final.csv'

    # Open and read the CSV file
    with open(csv_file_path, 'r') as file:
        csv_data = csv.reader(file)
        header = next(csv_data)  # Assuming the first row contains column headers
        for row in csv_data:
            # Assuming your CSV file columns match the table columns
            query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"
            session.execute(query, row)

    result = session.execute(f"select count(*) as count from {table_name}")
    print(f"Inserted {result.one().count} rows of data into {table_name} table\n")


def read_query():
    # Sample read query to display 5 entries in the table
    # Define your CQL SELECT query
    select_query = f"SELECT Over_no, Over_score, Short_comm, Innings_ID, Ball_ID FROM {table_name} LIMIT 5"  # Limiting to 10 rows for example

    # Execute the query
    rows = session.execute(select_query)
    print("Sample read query result: ")
    # Process the results
    for row in rows:
        # Access columns by their names
        print(f"Over_No: {row.over_no}, Over_Score: {row.over_score}, Short_comm: {row.short_comm}, "
              f"Innings_ID: {row.innings_id}, Ball_ID: {row.ball_id}")


def update_query():
    # Sample update query to update over_score & short comment

    # Query to show data before update.
    select_query = f"SELECT * FROM {table_name} WHERE Innings_ID = '1181768-1' and Ball_ID = '51'"
    # Execute the query
    rows = session.execute(select_query)
    # Process the results
    for row in rows:
        # Access columns by their names
        print(
            f"Result before update: \nOver_No: {row.over_no}, Over_Score: {row.over_score}, Short_comm: {row.short_comm}, "
            f"Commentary: {row.commentary}, Bold_Comm: {row.bold_comm}, Innings_ID: {row.innings_id}, "
            f"Ball_ID: {row.ball_id}, Match_ID: {row.match_id}")

    # Define your CQL UPDATE query
    update_query = f"UPDATE {table_name} SET Over_Score = '1', Short_comm = '[Chahar to Dhoni, 1 run]' WHERE Innings_ID = '1181768-1' and Ball_ID = '51'"  # Example update query

    # Execute the update query
    session.execute(update_query)

    # Execute the select query to show updated data
    rows = session.execute(select_query)
    # Process the results
    for row in rows:
        # Access columns by their names
        print(
            f"Result after update: \nOver_No: {row.over_no}, Over_Score: {row.over_score}, Short_comm: {row.short_comm}, "
            f"Commentary: {row.commentary}, Bold_Comm: {row.bold_comm}, Innings_ID: {row.innings_id}, "
            f"Ball_ID: {row.ball_id}, Match_ID: {row.match_id}")


def delete_query():
    # Sample delete query
    delete_query = f"DELETE FROM {table_name} WHERE Innings_ID = '1181768-1' and Ball_ID = '2'"

    # Execute the delete query
    session.execute(delete_query)

    result = session.execute(
        f"Select count(*) as count from {table_name}  WHERE Innings_ID = '1181768-1' and Ball_ID = '2'")
    if result.one().count == 0:
        print("\nRow with Innings_ID = '1181768-1' and Ball_ID = '2' deleted.\n")


def create_index():
    # Query to create index on Over_score, so that it can be added to where condition
    index_query = f"CREATE INDEX on {keyspace_name}.{table_name}(Over_Score)"

    session.execute(index_query)


def get_sixes_by_innings():
    # Execute the query
    select_query = (f"SELECT innings_id, "
                    f"COUNT(*) as sixes_count "
                    f"FROM {table_name} "
                    f"WHERE over_score = '6'"
                    f"GROUP BY innings_id;")

    rows = session.execute(select_query)

    print("Number of sixes in every Innings -")
    # Process the results
    for row in rows:
        print(f"Innings ID: {row.innings_id}, Sixes Count: {row.sixes_count}")


if __name__ == '__main__':
    # Authentication provider
    auth_provider = PlainTextAuthProvider(username=username, password=password)

    # Connect to Cassandra
    cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
    session = cluster.connect()

    # Create a keyspace
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}")

    # Set the keyspace
    session.set_keyspace(keyspace_name)

    create_table()
    insert_csv_file_data()
    read_query()
    update_query()
    delete_query()
    create_index()
    get_sixes_by_innings()
