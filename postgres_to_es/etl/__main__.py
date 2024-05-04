import time

from . import movies_database, search_engine
from .search_engine import Index

_MOVIES_DATABASE_UPDATES_CHECK_PERIOD_SECONDS = 10


def transfer_updated_entities() -> None:
    for filmworks_chunk in movies_database.get_updated_filmworks():
        search_engine.load(Index.MOVIES, filmworks_chunk)
    for genres_chunk in movies_database.get_updated_genres():
        search_engine.load(Index.GENRES, genres_chunk)
    for personas_chunk in movies_database.get_updated_personas():
        search_engine.load(Index.PERSONAS, personas_chunk)


if __name__ == "__main__":
    search_engine.create_indexes(Index.MOVIES, Index.GENRES, Index.PERSONAS)
    while True:
        transfer_updated_entities()
        time.sleep(_MOVIES_DATABASE_UPDATES_CHECK_PERIOD_SECONDS)
