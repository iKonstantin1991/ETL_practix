import os
from typing import Any, Dict, List, Optional, Union
from uuid import UUID
import json
from enum import Enum

import httpx
from pydantic import BaseModel, model_validator

from etl.backoff import backoff
from etl.logger import logger
from etl.settings import settings


class Index(str, Enum):
    MOVIES = "movies"
    GENRES = "genres"
    PERSONAS = "personas"


class SearchEngineFilmwork(BaseModel):
    id: UUID
    imdb_rating: Optional[float]
    genres: List[Dict[str, str]]
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


class SearchEngineGenre(BaseModel):
    id: UUID
    name: str


class PersonaFilmwork(BaseModel):
    id: UUID
    roles: List[str]


class SearchEnginePerson(BaseModel):
    id: UUID
    full_name: str
    films: List[PersonaFilmwork]

    @model_validator(mode="before")
    @classmethod
    def parse_films(cls, input_data: Dict[str, Any]) -> Dict[str, Any]:
        films = {}
        for pfw in input_data["films"]:
            fw_id, role = pfw["id"], pfw["role"]
            if fw_id in films:
                films[fw_id].roles.append(role)
            else:
                films[fw_id] = PersonaFilmwork(id=fw_id, roles=[role])

        input_data["films"] = films.values()
        return input_data


@backoff(exceptions=(httpx.RequestError,))
def load(index: Index,
         entity: List[Union[SearchEngineFilmwork, SearchEngineGenre, SearchEnginePerson]]) -> None:
    response = httpx.post(
        f"http://{settings.elastic_search_host}:{settings.elastic_search_port}/_bulk",
        content=_form_content(index, entity),
        headers={"Content-Type": "application/x-ndjson"}
    )
    response.raise_for_status()


def create_indexes(*indexes: Index) -> None:
    for index in indexes:
        _create_index(index)


def _create_index(index: Index) -> None:
    try:
        response = httpx.put(
            f"http://{settings.elastic_search_host}:{settings.elastic_search_port}/{index}",
            content=_get_index_schema(index),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        logger.info("Created new index: %s", index)
    except httpx.HTTPStatusError as e:
        if not e.response.status_code == httpx.codes.BAD_REQUEST:
            raise
        error_type = (e.response.json().get("error") or {}).get("type")
        if error_type != "resource_already_exists_exception":
            raise


def _get_index_schema(index: Index) -> str:
    with open(f"./schema_{index}_es.json", encoding="utf-8") as f:
        return f.read()


def _form_content(index: Index,
                  entities: List[Union[SearchEngineFilmwork, SearchEngineGenre, SearchEnginePerson]]) -> str:
    items = []
    for e in entities:
        items += [
            json.dumps({"index": {"_index": index, "_id": str(e.id)}}),
            e.json(),
        ]
    return os.linesep.join(items) + os.linesep
