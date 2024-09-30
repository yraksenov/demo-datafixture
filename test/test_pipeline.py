import os

import pandas as pd
import pytest


data_fixture_directories = ["data_fixtures"]


@pytest.mark.parametrize("data_fixture_dir", data_fixture_directories)
def test_data_pipeline(data_fixture_dir, testing_fixture):
    get_transformation, results = testing_fixture  # UNBOX the tuple
    transformation, root = get_transformation(data_fixture_dir)
    expected_data = pd.read_csv(os.path.join(data_fixture_dir, root["expectations"]))
    transformation.transform()

    results = pd.concat(results)
    pd.testing.assert_frame_equal(results, expected_data)
