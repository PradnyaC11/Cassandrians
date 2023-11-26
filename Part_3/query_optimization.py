import psycopg2
import time

DATABASE_NAME = "cassandrians"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432


def connect_potsgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
    Return the connection"""
    try:
        connection = psycopg2.connect(
            database=dbname, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        print("Connection established successfully for database " + dbname)
        return connection
    except:
        print("Error occurred while connecting to the database")


def query_optimization(curs):
    print(
        f"Time taken for the unoptimized query: {calculate_unoptimized_query_time_performace(curs)} seconds"
    )
    print(
        f"Time taken for the optimized query: {calculate_optimized_query_time_performace(curs)} seconds"
    )


def calculate_unoptimized_query_time_performace(curs):
    unoptimized_query = """SELECT 
    mt.match_id,
    mt.start_time,
    t1.team_name AS team_1_name,
    t2.team_name AS team_2_name,
    p1.player_name AS team_1_player,
    p2.player_name AS team_2_player
    FROM 
    match mt,
    team t1,
    team t2,
    match_player mp1,
    match_player mp2,
    players p1,
    players p2
    WHERE 
    mt.team_1 = t1.team_id
    AND mt.team_2 = t2.team_id
    AND mt.match_id = mp1.match_id
    AND mt.match_id = mp2.match_id
    AND mp1.player_id = p1.player_id
    AND mp2.player_id = p2.player_id;"""

    start_time = time.time()
    curs.execute(unoptimized_query)
    rows = curs.fetchall()
    end_time = time.time()
    time_taken = end_time - start_time
    return time_taken


def calculate_optimized_query_time_performace(curs):
    optimized_query = """SELECT 
    mt.match_id,
    mt.start_time,
    t1.team_name AS team_1_name,
    t2.team_name AS team_2_name,
    p1.player_name AS team_1_player,
    p2.player_name AS team_2_player
    FROM match mt
    JOIN team t1 ON mt.team_1 = t1.team_id
    JOIN team t2 ON mt.team_2 = t2.team_id
    JOIN match_player mp1 ON mt.match_id = mp1.match_id
    JOIN match_player mp2 ON mt.match_id = mp2.match_id
    JOIN players p1 ON mp1.player_id = p1.player_id
    JOIN players p2 ON mp2.player_id = p2.player_id;"""

    start_time = time.time()
    curs.execute(optimized_query)
    rows = curs.fetchall()
    end_time = time.time()
    time_taken = end_time - start_time
    return time_taken


if __name__ == "__main__":
    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        curs = conn.cursor()
        query_optimization(curs)
