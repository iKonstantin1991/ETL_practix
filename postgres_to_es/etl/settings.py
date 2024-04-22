from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int

    redis_host: str
    redis_port: int
    redis_db: int

    elastic_search_host: str
    elastic_search_port: int

    model_config = SettingsConfigDict(env_file='.env')


settings = Settings()
