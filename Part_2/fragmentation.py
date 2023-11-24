import psycopg2
import csv
#pylint:skip-file

DATABASE_NAME = 'cassandrians'
DB_USER = "postgres"
# DB_PASS = "postgres"
DB_PASS = "Happyplace11*"
DB_HOST = "localhost"
DB_PORT = 5432
VENUE_TABLE = 'venue'
TEAM_TABLE = "team"
MATCH_TABLE = "match"
MATCH_PLAYER_TABLE = "match_player"
MATCH_OFFICIALS_TABLE = "match_officials"
PLAYERS_TABLE = "players"
BOWL_BY_BOWL_TABLE = "bowl_by_bowl"


def create_database(dbname):
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named {DATABASE_NAME}
       Close the connection"""
    conn = connect_potsgres("postgres")
    curs = conn.cursor()
    conn.autocommit = True
    sql_create_database = "create database  " + dbname
    curs.execute(sql_create_database)
    conn.close()


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


def range_partition(curs):

    table = f"CREATE TABLE IF NOT EXISTS runs_per_ball (batsman_id int, bowl_no int, runs_by_batter int) partition by range(runs_by_batter);"
    curs.execute(table)
    conn.commit()

    curs.execute(f"CREATE TABLE IF NOT EXISTS runs_below_3 PARTITION OF runs_per_ball FOR VALUES FROM ('1') TO ('3'); ")
    conn.commit()

    curs.execute(f"CREATE TABLE IF NOT EXISTS runs_boundaries PARTITION OF runs_per_ball FOR VALUES FROM ('4') TO ('6'); ")
    conn.commit()

if __name__ == '__main__':
    create_database(DATABASE_NAME)

    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        list_partition(curs)
        range_partition(curs)