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


def distributed_indexing_optimization(curs):
    print(
        f"Time taken for the Ball by ball data query Before Indexing: {calculate_ball_by_ball_time_performace(curs)} seconds"
    )
    add_indexing(curs)
    print(
        f"Time taken for the Ball by ball data query After Indexing: {calculate_ball_by_ball_time_performace(curs)} seconds"
    )


def add_indexing(curs):
    indexing_query = """CREATE INDEX idx_match_venue_id ON match(venue_id);
    CREATE INDEX idx_match_team_1 ON match(team_1);
    CREATE INDEX idx_match_team_2 ON match(team_2);
    CREATE INDEX idx_match_team_1_captain_id ON match(team_1_captain_id);
    CREATE INDEX idx_match_team_2_captain_id ON match(team_2_captain_id);
    CREATE INDEX idx_match_umpire_id_1 ON match(umpire_id_1);
    CREATE INDEX idx_match_umpire_id_2 ON match(umpire_id_2);
    CREATE INDEX idx_match_umpire_id_3 ON match(umpire_id_3);
    CREATE INDEX idx_players_batsman_id ON players(player_id);
    CREATE INDEX idx_players_bowler_id ON players(player_id);
    CREATE INDEX idx_players_non_striker_id ON players(player_id);
    CREATE INDEX idx_players_catcher_id ON players(player_id);
    CREATE INDEX idx_players_player_out_id ON players(player_id);
    CREATE INDEX idx_bowl_by_bowl_match_id ON bowl_by_bowl(match_id);
    CREATE INDEX idx_bowl_by_bowl_batsman_id ON bowl_by_bowl(batsman_id);
    CREATE INDEX idx_bowl_by_bowl_bowler_id ON bowl_by_bowl(bowler_id);
    CREATE INDEX idx_bowl_by_bowl_non_striker_id ON bowl_by_bowl(non_striker_id);
    CREATE INDEX idx_bowl_by_bowl_catcher_id ON bowl_by_bowl(catcher_id);
    CREATE INDEX idx_bowl_by_bowl_player_out_id ON bowl_by_bowl(player_out_id);"""
    curs.execute(indexing_query)


def calculate_ball_by_ball_time_performace(curs):
    optimized_query = """SELECT
    m.match_id,
    m.start_time,
    v.address1,
    t1.team_name AS team_1,
    t2.team_name AS team_2,
    p1.player_name AS team_1_captain,
    p2.player_name AS team_2_captain,
    mo1.umpire_name AS umpire_1,
    mo2.umpire_name AS umpire_2,
    mo3.umpire_name AS umpire_3,
    b.match_id AS bowl_by_bowl_match_id,
    b.inning_id,
    b.bowl_no,
    b.over_no,
    b.wicket,
    b.runs_by_batter,
    b.runs_by_extras,
    bp.player_name AS batsman_name,
    bo.player_name AS bowler_name,
    nb.player_name AS non_striker_name,
    ca.player_name AS catcher_name,
    po.player_name AS player_out_name
    FROM match m
    JOIN venue v ON v.venue_id = m.venue_id
    JOIN team t1 ON t1.team_id = m.team_1
    JOIN team t2 ON t2.team_id = m.team_2
    JOIN players p1 ON p1.player_id = m.team_1_captain_id
    JOIN players p2 ON p2.player_id = m.team_2_captain_id
    JOIN match_officials mo1 ON mo1.umpire_id = m.umpire_id_1
    JOIN match_officials mo2 ON mo2.umpire_id = m.umpire_id_2
    JOIN match_officials mo3 ON mo3.umpire_id = m.umpire_id_3
    JOIN bowl_by_bowl b ON b.match_id = m.match_id
    JOIN players bp ON bp.player_id = b.batsman_id
    JOIN players bo ON bo.player_id = b.bowler_id
    JOIN players nb ON nb.player_id = b.non_striker_id
    LEFT JOIN players ca ON ca.player_id = b.catcher_id
    LEFT JOIN players po ON po.player_id = b.player_out_id;"""

    start_time = time.time()
    curs.execute(optimized_query)
    rows = curs.fetchall()
    end_time = time.time()
    time_taken = end_time - start_time
    return time_taken


if __name__ == "__main__":
    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        curs = conn.cursor()
        distributed_indexing_optimization(curs)
