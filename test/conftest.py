import json
import os
from unittest.mock import Mock

import pandas as pd
import pytest

from demo import pipeline


class MockedConnection:
    class MockedContext:
        def __enter__(self):
            return Mock()

        def __exit__(self, exc_type, exc_value, exc_traceback):
            return False

    def __call__(self):
        return self.MockedContext()


@pytest.fixture
def testing_fixture(monkeypatch):
    results = []

    def get_transformation(data_fixture_dir):
        with open(os.path.join(data_fixture_dir, "root.json"), encoding="utf-8") as fd:
            root = json.load(fd)

        def extract_mock(connection):
            # assumed a series of data frames
            yield pd.read_csv(os.path.join(data_fixture_dir, root["input_data"]))

        monkeypatch.setattr(pipeline, "extract", extract_mock)
        monkeypatch.setattr(pipeline.Producer, "get_connection", MockedConnection())

        transformation = pipeline.Producer()

        def load_mock(_, connection, result):
            nonlocal results
            results.append(result)  # or pd.concat

        monkeypatch.setattr(pipeline.Producer, "load", load_mock)
        return transformation, root

    return get_transformation, results
