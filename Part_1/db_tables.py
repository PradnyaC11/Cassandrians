import psycopg2
import csv
#pylint:skip-file

DATABASE_NAME = 'cassandrians'
DB_USER = "postgres"
DB_PASS = "postgres"
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
    sql_create_database = "create database " + dbname
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


def create_venue_table(curs):
    create_venue_table_query = ("create table " + VENUE_TABLE + " (" +
                                "venue_id SERIAL PRIMARY KEY, " +
                                "address1 text NOT NULL, " +
                                "address2 text, " +
                                "city text NOT NULL, " +
                                "state text NOT NULL, " +
                                "country text NOT NULL)"
                                )
    curs.execute(create_venue_table_query)


def create_team_table(curs):
    create_team_table_query = ("create table " + TEAM_TABLE + " (" +
                               "team_id SERIAL PRIMARY KEY, " +
                               "team_name text NOT NULL, " +
                               "coach text NOT NULL, " +
                               "home_ground int NOT NULL, " +
                               "owner text NOT NULL, " +
                               "CONSTRAINT fk_home_ground FOREIGN KEY(home_ground) REFERENCES venue(venue_id)" +
                               ")"
                               )
    curs.execute(create_team_table_query)


def create_match_table(curs):
    create_match_table_query = ("create table " + MATCH_TABLE + " (" +
                                "match_id SERIAL PRIMARY KEY, " +
                                "venue_id int NOT NULL, " +
                                "start_time timestamptz NOT NULL, " +
                                "team_1 int NOT NULL, " +
                                "team_2 int NOT NULL, " +
                                "team_1_captain_id int NOT NULL, " +
                                "team_2_captain_id int NOT NULL, " +
                                "toss_winner int NOT NULL, " +
                                "toss_decision text NOT NULL, " +
                                "toss_outcome text NOT NULL, " +
                                "umpire_id_1 int NOT NULL, " +
                                "umpire_id_2 int NOT NULL, " +
                                "umpire_id_3 int NOT NULL," +
                                "CONSTRAINT fk_venue_id FOREIGN KEY(venue_id) REFERENCES venue(venue_id)," +
                                "CONSTRAINT fk_team_1 FOREIGN KEY(team_1) REFERENCES team(team_id)," +
                                "CONSTRAINT fk_team_2 FOREIGN KEY(team_2) REFERENCES team(team_id)," +
                                "CONSTRAINT fk_team_1_captain_id FOREIGN KEY(team_1_captain_id) REFERENCES players(player_id), " +
                                "CONSTRAINT fk_team_2_captain_id FOREIGN KEY(team_2_captain_id) REFERENCES players(player_id), " +
                                "CONSTRAINT fk_toss_winner FOREIGN KEY(toss_winner) REFERENCES team(team_id)," +
                                "CONSTRAINT fk_umpire_id_1 FOREIGN KEY(umpire_id_1) REFERENCES match_officials(umpire_id)," +
                                "CONSTRAINT fk_umpire_id_2 FOREIGN KEY(umpire_id_2) REFERENCES match_officials(umpire_id)," +
                                "CONSTRAINT fk_umpire_id_3 FOREIGN KEY(umpire_id_3) REFERENCES match_officials(umpire_id)" +
                                ")"
                                )
    curs.execute(create_match_table_query)


def create_match_player_table(curs):
    create_match_player_table_query = ("create table " + MATCH_PLAYER_TABLE + " (" +
                                       "match_id int, " +
                                       "player_id int, " +
                                       "PRIMARY KEY (match_id, player_id), " +
                                       "CONSTRAINT fk_match_id FOREIGN KEY(match_id) REFERENCES match(match_id)," +
                                       "CONSTRAINT fk_player_id FOREIGN KEY(player_id) REFERENCES players(player_id)" +
                                       ") "
                                       )
    curs.execute(create_match_player_table_query)


def create_match_officials_table(curs):
    create_match_officials_table_query = ("create table " + MATCH_OFFICIALS_TABLE + " (" +
                                          "umpire_id SERIAL PRIMARY KEY, " +
                                          "umpire_name text NOT NULL)"
                                          )
    curs.execute(create_match_officials_table_query)


def create_players_table(curs):
    create_players_table_query = ("create table " + PLAYERS_TABLE + " (" +
                                  "player_id SERIAL PRIMARY KEY, " +
                                  "player_name text NOT NULL, " +
                                  "team_id int NOT NULL, " +
                                  "speciality text NOT NULL, " +
                                  "CONSTRAINT team_id FOREIGN KEY(team_id) REFERENCES team(team_id)" +
                                  ")"
                                  )
    curs.execute(create_players_table_query)


def create_bowl_by_bowl_table(curs):
    create_bowl_by_bowl_table_query = ("create table " + BOWL_BY_BOWL_TABLE + " (" +
                                       "match_id int NOT NULL, " +
                                       "inning_id int NOT NULL, " +
                                       "bowl_no int NOT NULL, " +
                                       "over_no int NOT NULL, " +
                                       "wicket boolean, " +
                                       "runs_by_batter int DEFAULT 0, " +
                                       "runs_by_extras int DEFAULT 0, " +
                                       "batsman_id int NOT NULL, " +
                                       "bowler_id int NOT NULL, " +
                                       "non_striker_id int, " +
                                       "wicket_type text, " +
                                       "catcher_id int, " +
                                       "player_out_id int, " +
                                       "PRIMARY KEY (match_id, inning_id, bowl_no, over_no), " +
                                       "CONSTRAINT fk_match_id FOREIGN KEY(match_id) REFERENCES match(match_id)," +
                                       "CONSTRAINT fk_batsman_id FOREIGN KEY(batsman_id) REFERENCES players(player_id)," +
                                       "CONSTRAINT fk_bowler_id FOREIGN KEY(bowler_id) REFERENCES players(player_id)," +
                                       "CONSTRAINT fk_non_striker_id FOREIGN KEY(non_striker_id) REFERENCES players(player_id)," +
                                       "CONSTRAINT fk_catcher_id FOREIGN KEY(catcher_id) REFERENCES players(player_id)," +
                                       "CONSTRAINT fk_player_out_id FOREIGN KEY(player_out_id) REFERENCES players(player_id)" +
                                       ")"
                                       )
    curs.execute(create_bowl_by_bowl_table_query)

def insert_data_into_table(curs, table_name):
    data = read_csv_datacsvObject = csv.reader(open(r'Part_1/data/'+table_name+'.csv', 'r'), dialect = 'excel',  delimiter = ',')
    next(data)
    # Get the column names from the table
    curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
    column_names = [row[0] for row in curs.fetchall()]
    # Insert the data into the table
    for row in data:
        # Insert None for NULL values
        for i, value in enumerate(row):
            if value == "":
                row[i] = None
        # Insert the values into the table
        curs.execute("INSERT INTO %s (%s) VALUES (%s)" % (table_name, ", ".join(column_names), ", ".join(["%s" for _ in range(len(column_names))])), row)

def insert_data(curs):
    insert_data_into_table(curs, VENUE_TABLE)
    insert_data_into_table(curs, TEAM_TABLE)
    insert_data_into_table(curs, MATCH_OFFICIALS_TABLE)
    insert_data_into_table(curs, PLAYERS_TABLE)
    insert_data_into_table(curs, MATCH_TABLE)
    insert_data_into_table(curs, MATCH_PLAYER_TABLE)
    insert_data_into_table(curs, BOWL_BY_BOWL_TABLE)

def create_tables(curs):
    create_venue_table(curs)
    create_team_table(curs)
    create_match_officials_table(curs)
    create_players_table(curs)
    create_match_table(curs)
    create_match_player_table(curs)
    create_bowl_by_bowl_table(curs)




if __name__ == '__main__':
    create_database(DATABASE_NAME)

    with connect_potsgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        create_tables(curs)
        insert_data(curs)
        