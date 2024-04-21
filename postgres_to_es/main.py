import time

from etl import movies_database, search_engine

_MOVIES_DATABASE_UPDATES_CHECK_PERIOD_SECONDS = 5


def main() -> None:
    for filmworks_chunk in movies_database.get_updated_filmworks():
        search_engine.load(filmworks_chunk)


if __name__ == "__main__":
    while True:
        main()
        time.sleep(_MOVIES_DATABASE_UPDATES_CHECK_PERIOD_SECONDS)
