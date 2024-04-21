import os
from typing import Any, Dict, List, Optional
from uuid import UUID
import json

import httpx
from pydantic import BaseModel, model_validator
from dotenv import load_dotenv

from etl.backoff import backoff

load_dotenv()

_SEARCH_ENGINE_HOST = os.environ.get('ELASTIC_SEARCH_HOST')
_SEARCH_ENGINE_PORT = os.environ.get('ELASTIC_SEARCH_PORT')


class SearchEngineFilmwork(BaseModel):
    id: UUID
    imdb_rating: Optional[float]
    genres: List[str]
    title: str
    description: Optional[str]
    directors_names: List[str]
    actors_names: List[str]
    writers_names: List[str]
    directors: List[Dict[str, str]]
    actors: List[Dict[str, str]]
    writers: List[Dict[str, str]]

    @model_validator(mode="before")
    @classmethod
    def parse_personas(cls, input_data: Dict[str, Any]) -> Dict[str, Any]:
        for role in ("director", "actor", "writer"):
            personas = [p for p in input_data["personas"] if p["role"] == role]
            input_data[f"{role}s_names"] = [p["name"] for p in personas]
            input_data[f"{role}s"] = [{"id": p["id"], "name": p["name"]} for p in personas]
        return input_data


@backoff(exceptions=(httpx.RequestError,))
def load(filmworks: List[SearchEngineFilmwork]) -> None:
    response = httpx.post(
        f"http://{_SEARCH_ENGINE_HOST}:{_SEARCH_ENGINE_PORT}/_bulk",
        content=_form_data(filmworks),
        headers={"Content-Type": "application/x-ndjson"}
    )
    response.raise_for_status()


def _form_data(filmworks: List[SearchEngineFilmwork]) -> str:
    filmworks_items = []
    for fw in filmworks:
        filmworks_items += [
            json.dumps({"index": {"_index": "movies", "_id": str(fw.id)}}),
            fw.json(),
        ]
    return os.linesep.join(filmworks_items) + os.linesep
