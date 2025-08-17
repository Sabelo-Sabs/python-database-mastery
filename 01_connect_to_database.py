# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: python-database-mastery
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Setup SQLAlchemy Connection
# 
# - Import SQLAlchemy modules
# - Define the database connection URL
# - Create a connection "engine" that talks to the database

# %%
from sqlalchemy import create_engine, URL

# Build the database URL with username, password, host, database name, and port
url = URL.create(
    drivername="postgresql+psycopg2",  # driver = database type + driver library
    username='testuser',               # PostgreSQL username
    password='testpassword',           # PostgreSQL password
    host='localhost',                  # Host address (localhost if running on same machine)
    database='testuser',               # Database name
    port=5432                          # PostgreSQL default port
)

# Create the database engine
engine = create_engine(url, echo=True)  # echo=True will print all SQL executed (good for learning)

# %% [markdown]
# ## Preview the Connection URL
# 
# - Display the connection string (with password hidden)
# - Useful to double-check if URL was built correctly

# %%
url.render_as_string()

# %% [markdown]
# ## Setup SQLAlchemy Session
# 
# - Create a `Session` factory bound to the engine
# - Use a session to execute a raw SQL command inside a transaction

# %%
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# Create a session factory (like a template for sessions)
Session = sessionmaker(engine)

# Open a new session, execute a raw SQL command, and commit the transaction
with Session() as session:
    # Here you would normally add, delete or query objects
    session.execute(text("some raw SQL"))  # Replace "some raw SQL" with real SQL
    session.commit()  # Save changes to the database
# Session is automatically closed after exiting the "with" block
