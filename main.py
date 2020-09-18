import json
from fastapi import FastAPI

import pandas as pd
import requests
import redis
import uvicorn

app = FastAPI()
redis_conn = redis.Redis(host='localhost', port=6379, db=0)

CACHE_REFRESH_TIMEOUT = 60  # measure in seconds
BASE_URL = 'https://ghibliapi.herokuapp.com'


class GhibliAPI:
    @classmethod
    def request(cls, url, params):
        data = requests.get(url, params=params).json()
        return data

    @classmethod
    def get_people(cls):
        return cls.request(f'{BASE_URL}/people/', {'fields': 'name,films'})

    @classmethod
    def get_films(cls):
        return cls.request(f'{BASE_URL}/films/', {'fields': 'id,title'})


class ProcessData:
    @staticmethod
    def get_people_df(people: dict):
        people_df = pd.DataFrame(people)
        people_df = people_df.explode('films').rename(columns={"films": "film_id"})
        people_df['film_id'] = people_df['film_id'].str.replace(
            'https://ghibliapi.herokuapp.com/films/', ''
        )
        return people_df

    @staticmethod
    def get_films_df(films: dict):
        return pd.DataFrame(films)

    @classmethod
    def process(cls, films, people):
        films_df = cls.get_films_df(films)
        people_df = cls.get_people_df(people)
        all_info = pd.merge(
            films_df, people_df, left_on='id', right_on='film_id', how='outer'
        )
        grouped_films = all_info.groupby(['id', 'title'])['name'].apply(
            lambda x: list(x) if x.any() else []
        )  # grouping people by film, empty list if no people for film found

        grouped_df = pd.DataFrame(
            {
                'film': grouped_films.index.get_level_values('title'),
                'people': grouped_films.values,
            }
        )

        json_data = grouped_df.to_json(orient='records')
        return json_data


def refresh_cache():
    films = GhibliAPI.get_films()
    people = GhibliAPI.get_people()
    json_data = ProcessData.process(films, people)
    redis_conn.set('all_films', json_data, ex=CACHE_REFRESH_TIMEOUT)


@app.get("/films")
def get_films():
    while True:
        all_films_json = redis_conn.get('all_films')
        if all_films_json:
            break
        refresh_cache()

    return json.loads(all_films_json)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)  # for debug purposes
