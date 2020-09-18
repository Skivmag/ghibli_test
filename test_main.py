import json

import pandas as pd
import pytest
from starlette.testclient import TestClient

from fixtures.expected_data import PEOPLE_CLEANED, FILMS_CLEANED
from main import app, ProcessData

client = TestClient(app)


@pytest.fixture
def people_data():
    with open('fixtures/people.json') as json_file:
        data = json.load(json_file)
    return data


@pytest.fixture
def films_data():
    with open('fixtures/films.json') as json_file:
        data = json.load(json_file)
    return data


@pytest.fixture
def final_data():
    with open('fixtures/films_with_people.json') as json_file:
        data = json.load(json_file)
    return data


@pytest.fixture
def people_df():
    return pd.DataFrame(PEOPLE_CLEANED)


@pytest.fixture
def films_df():
    return pd.DataFrame(FILMS_CLEANED)


def test_film_url():
    response = client.get("/films")
    assert response.status_code == 200


class TestProcessing:
    def test_people_df(self, people_data, people_df):
        frame = ProcessData.get_people_df(people_data)
        pd.testing.assert_frame_equal(frame, people_df)

    def test_films_df(self, films_data, films_df):
        frame = ProcessData.get_films_df(films_data)
        pd.testing.assert_frame_equal(frame, films_df)

    def test_process(self, films_data, people_data, final_data):
        json_data = ProcessData.process(films_data, people_data)
        assert json.loads(json_data) == final_data
