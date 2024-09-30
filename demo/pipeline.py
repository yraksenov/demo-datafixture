import os
import sqlite3

import pandas as pd


INPUT_SQL = "SELECT v1, op, v2 FROM INPUT;"


def extract(connection):
    for chunk in pd.read_sql_query(INPUT_SQL, connection, chunksize=1000):
        yield chunk


class Producer:
    def get_connection(self):
        database_path = os.path.join("data", "database.db")
        return sqlite3.connect(database_path)

    def load(self, connection, result):
        return result.to_sql("OUTPUT", connection, index=False, if_exists="append")

    def transform(self):
        with self.get_connection() as connection:
            for chunk in extract(connection=connection):
                result = self._process(chunk)
            self.load(connection, result)

    def _process(self, chunk):
        """just a demo transformation"""
        op_plus = chunk.loc[chunk["op"] == "+"]
        chunk["plus"] = op_plus["v1"] + op_plus["v2"]
        op_minus = chunk.loc[chunk["op"] == "-"]
        chunk["minus"] = op_minus["v1"] - op_minus["v2"]
        op_mul = chunk.loc[chunk["op"] == "*"]
        chunk["mul"] = op_mul["v1"] * op_mul["v2"]
        op_div = chunk.loc[chunk["op"] == "/"]
        chunk["div"] = op_div["v1"] / op_div["v2"]
        chunk["result"] = chunk["plus minus mul div".split()].agg("max", axis="columns")
        return chunk["v1 op v2 result".split()]


def main():
    producer = Producer()
    producer.transform()


if __name__ == "__main__":
    main()
    print("The data transformation done successfully.")
