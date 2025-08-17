from logging.config import fileConfig
from environs import Env

env = Env()
env.read_env()

from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine.url import URL  # <-- import URL

from alembic import context
from working_with_databases_02 import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# --- 🔧 Override database URL programmatically ---
url = URL.create(
    drivername="postgresql+psycopg2",  # driver name = postgresql + psycopg2
    username=env.str('POSTGRES_USER'),
    password=env.str('POSTGRES_PASSWORD'),
    host=env.str('DATABASE_HOST'),
    database=env.str('POSTGRES_DB'),
    port=5432
)

config.set_main_option('sqlalchemy.url', str(url))
# ------------------------------------------------


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
