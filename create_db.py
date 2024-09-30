import os
import sqlite3

import pandas as pd


SQL_CREATE_INPUT = """
    CREATE TABLE IF NOT EXISTS INPUT (
        v1 INT NOT NULL, 
        op CHAR(1) NOT NULL,
        v2 INT NOT NULL
    );
"""


SQL_CREATE_OUTPUT = """
    CREATE TABLE IF NOT EXISTS OUTPUT (
        v1 INT NOT NULL, 
        op CHAR(1) NOT NULL,
        v2 INT NOT NULL,
        result REAL NOT NULL
    );
"""


def main(database_path):
    df = pd.read_csv(os.path.join("data_fixtures", "input_data.csv"))
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute(SQL_CREATE_INPUT)
        cursor.execute(SQL_CREATE_OUTPUT)
        connection.commit()
        df["v1 op v2".split()].to_sql(
            "INPUT", connection, index=False, if_exists="append"
        )


if __name__ == "__main__":
    database_dir, database_filename = "data", "database.db"
    database_path = os.path.join(database_dir, database_filename)
    os.makedirs(database_dir, exist_ok=True)
    main(database_path)
    print(database_path, "database successfully created.")
