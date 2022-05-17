from pydantic import BaseSettings


class Settings(BaseSettings):
    postgres_host: str = "postgresql://postgres@localhost:5432/{}"
    postgres_db: str = "postgres"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    datetime_format: str = "%Y-%m-%d %H:%M:%S.%f"
    debug: bool = False


settings = Settings()
