import os
from typing import Any, Dict, List, Optional
from uuid import UUID
import json

import httpx
from pydantic import BaseModel, model_validator

from etl.backoff import backoff
from etl.logger import logger
from etl.settings import settings

_INDEX_NAME = "movies"


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
        f"http://{settings.elastic_search_host}:{settings.elastic_search_port}/_bulk",
        content=_form_content(filmworks),
        headers={"Content-Type": "application/x-ndjson"}
    )
    response.raise_for_status()


def create_index() -> None:
    try:
        response = httpx.put(
            f"http://{settings.elastic_search_host}:{settings.elastic_search_port}/{_INDEX_NAME}",
            content=_get_index_schema(),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        logger.info("Created new index: %s", _INDEX_NAME)
    except httpx.HTTPStatusError as e:
        if not e.response.status_code == httpx.codes.BAD_REQUEST:
            raise
        error_type = (e.response.json().get("error") or {}).get("type")
        if error_type != "resource_already_exists_exception":
            raise


def _get_index_schema() -> str:
    with open("./schema_movies_es.json", encoding="utf-8") as f:
        return f.read()


def _form_content(filmworks: List[SearchEngineFilmwork]) -> str:
    filmworks_items = []
    for fw in filmworks:
        filmworks_items += [
            json.dumps({"index": {"_index": _INDEX_NAME, "_id": str(fw.id)}}),
            fw.json(),
        ]
    return os.linesep.join(filmworks_items) + os.linesep
