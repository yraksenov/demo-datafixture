import json
import os
import random

import pandas as pd


def row_generator(count):
    operations = "+-/*"
    for _ in range(count):
        val1 = random.randint(1, 100)
        operation = random.choice(operations)
        val2 = random.randint(1, 100)
        yield val1, operation, val2, eval(f"{val1} {operation} {val2}")


INPUT_DATA = "input_data.csv"
EXPECTATIONS = "expectations.csv"


def main(fixtures_dir, count):
    df = pd.DataFrame(row_generator(count), columns="v1 op v2 result".split())
    expectations_filename = os.path.join(fixtures_dir, EXPECTATIONS)
    input_data_filename = os.path.join(fixtures_dir, INPUT_DATA)
    with open(os.path.join(fixtures_dir, "root.json"), "w") as fd:
        json.dump({"input_data": INPUT_DATA, "expectations": EXPECTATIONS}, fd)
    df.to_csv(expectations_filename, index=False)
    df["v1 op v2".split()].to_csv(input_data_filename)


if __name__ == "__main__":
    fixtures_dir = "data_fixtures"
    os.makedirs(fixtures_dir, exist_ok=True)
    main(fixtures_dir, 1000)
    print(fixtures_dir, "directory successfully created.")
