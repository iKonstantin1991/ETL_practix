import os
from typing import Any, Dict, List, Iterator, Optional
from uuid import UUID
from datetime import datetime

from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

from etl.search_engine import SearchEngineFilmwork
from etl.logger import logger
from etl.backoff import backoff
from etl import state

load_dotenv()

_CHUNK_SIZE = 100

conninfo = ("postgresql://"
            f"{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@"
            f"{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/"
            f"{os.environ.get('POSTGRES_DB')}")
conn_pool = ConnectionPool(conninfo, min_size=1, max_size=1)


def get_updated_filmworks() -> Iterator[List[SearchEngineFilmwork]]:
    yield from _get_filmorks_with_updated_personas()
    yield from _get_filmorks_with_updated_genres()
    yield from _get_updated_filmworks()


def _get_filmorks_with_updated_personas() -> Iterator[List[SearchEngineFilmwork]]:
    logger.info("Getting filmworks with updated personas")
    yield from _get_filmorks_with_updated_related_entities("person")


def _get_filmorks_with_updated_genres() -> Iterator[List[SearchEngineFilmwork]]:
    logger.info("Getting filmworks with updated genres")
    yield from _get_filmorks_with_updated_related_entities("genre")


def _get_updated_filmworks() -> Iterator[List[SearchEngineFilmwork]]:
    logger.info("Getting updated filmworks")
    state_key = _get_state_key("filmwork")
    seen_filmworks_count = 0
    should_generate = True
    while should_generate:
        last_seen_modified = state.get(state_key)
        cmd = _build_sql_requesting_filmworks(last_seen_modified=last_seen_modified)
        filmworks = _db_execute(cmd)
        if filmworks:
            state.save(state_key, filmworks[-1]["modified"])
            seen_filmworks_count += len(filmworks)
            yield _normalize_filmworks(filmworks)
        else:
            should_generate = False
            logger.info(f"Seen {seen_filmworks_count} updates for filmwork: {last_seen_modified}")


def _get_filmorks_with_updated_related_entities(entity: str) -> Iterator[List[SearchEngineFilmwork]]:
    for entity_ids_ids in _get_updated_related_entities_ids(entity):
        for filmwork_ids in _get_filmworks_ids_with_related_entities(entity, entity_ids_ids):
            yield _get_filmworks_by_ids(filmwork_ids)


def _get_updated_related_entities_ids(entity: str) -> Iterator[List[UUID]]:
    state_key = _get_state_key(entity)
    seen_entities_count = 0
    should_generate = True
    while should_generate:
        last_seen_modified = state.get(state_key)
        cmd = _build_sql_requesting_entity(entity, last_seen_modified)
        entities = _db_execute(cmd)
        if entities:
            state.save(state_key, entities[-1]["modified"])
            seen_entities_count += len(entities)
            yield [e["id"] for e in entities]
        else:
            should_generate = False
            logger.info(f"Seen {seen_entities_count} updates for {entity}: {last_seen_modified}")


def _get_filmworks_ids_with_related_entities(entity: str, entity_ids: List[UUID]) -> Iterator[List[UUID]]:
    should_generate = True
    state_key = _get_state_key(f"{entity}_filmwork")
    while should_generate:
        last_seen_modified = state.get(state_key)
        cmd = _build_sql_requesting_filmworks_ids_with_entity(entity, entity_ids, last_seen_modified)
        filmworks = _db_execute(cmd)
        if filmworks:
            state.save(state_key, filmworks[-1]["modified"])
            yield [fw["id"] for fw in filmworks]
        else:
            state.reset(state_key)
            should_generate = False


def _get_filmworks_by_ids(filmwork_ids: List[UUID]) -> List[SearchEngineFilmwork]:
    cmd = _build_sql_requesting_filmworks(filmwork_ids)
    return _normalize_filmworks(_db_execute(cmd))


@backoff(exceptions=(OperationalError,))
def _db_execute(cmd: str) -> Dict[str, Any]:
    with conn_pool.connection() as conn:
        conn.row_factory=dict_row
        return conn.execute(cmd).fetchall()


def _normalize_filmworks(filmworks: List[Dict[str, Any]]) -> List[SearchEngineFilmwork]:
    return [SearchEngineFilmwork.parse_obj(fw) for fw in filmworks]


def _build_sql_requesting_entity(entity: str, last_seen_modified: datetime) -> str:
    return f"""
        SELECT DISTINCT id, modified
        FROM content.{entity}
        WHERE modified > '{last_seen_modified.isoformat()}'
        ORDER BY modified
        LIMIT {_CHUNK_SIZE}
    """


def _build_sql_requesting_filmworks_ids_with_entity(
        entity: str, entity_ids: List[UUID], last_seen_modified: datetime) -> str:
    normalized_entity_ids = ' ,'.join([f"'{str(id)}'" for id in entity_ids])
    return f"""
        SELECT DISTINCT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.{entity}_film_work efw ON efw.film_work_id = fw.id
        WHERE efw.{entity}_id IN ({normalized_entity_ids}) AND
              fw.modified > '{last_seen_modified.isoformat()}'
        ORDER BY fw.modified
        LIMIT {_CHUNK_SIZE}
    """


def _build_sql_requesting_filmworks(ids: Optional[List[UUID]] = None,
                                    last_seen_modified: datetime = None) -> str:
    if ids:
        normalized_ids = ' ,'.join([f"'{str(p)}'" for p in ids])
        condition, limit = f"WHERE fw.id IN ({normalized_ids})", ""
    else:
        condition, limit = f"WHERE fw.modified > '{last_seen_modified.isoformat()}'", f"LIMIT {_CHUNK_SIZE}"
    return f"""
        SELECT fw.id,
               fw.title,
               fw.description,
               fw.rating as imdb_rating,
               fw.modified,
               COALESCE(json_agg(DISTINCT jsonb_build_object('role', pfw.role,
                                                             'id', p.id,
                                                             'name', p.full_name)) FILTER (WHERE p.id is not null),
                        '[]') as personas,
               array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        {condition}
        GROUP BY fw.id
        ORDER BY fw.modified
        {limit}
    """


def _get_state_key(prefix: str) -> str:
    return f"{prefix}_last_seen_modified"
