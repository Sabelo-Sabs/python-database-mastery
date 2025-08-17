# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: python-database-mastery-ybCYjRg8
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Section 3 - Alembic for Database Management
# ## Initializing Connection to Database
#
# Firstly, lets once again initialize the database connection (copy from the previous section)

# %%
from sqlalchemy import create_engine, URL
from sqlalchemy.orm import sessionmaker

url = URL.create(
    drivername="postgresql+psycopg2",  # driver name = postgresql + the library we are using (psycopg2)
    username='testuser',
    password='testpassword',
    host='localhost',
    database='testuser',
    port=5432
)

engine = create_engine(url, echo=True)
session_pool = sessionmaker(bind=engine)

# %% [markdown]
# If you for some reason skipped the previous section, you can run the following code to declare the tables.

# %%
from typing_extensions import Annotated
from typing import Optional
from sqlalchemy.ext.declarative import declared_attr

from datetime import datetime
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import ForeignKey, BIGINT, String, INTEGER

from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from sqlalchemy.sql.functions import func


# Creating a base class
class Base(DeclarativeBase):
    pass

# Users ForeignKey
user_fk = Annotated[
    int, mapped_column(BIGINT, ForeignKey("users.telegram_id", ondelete="CASCADE"))
]

# integer primary key
int_pk = Annotated[int, mapped_column(INTEGER, primary_key=True)]

# string column with length 255
str_255 = Annotated[str, mapped_column(String(255))]


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    
# So now your Users class will look like this:
class Users(Base, TimestampMixin, TableNameMixin):

    telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=False)
    full_name: Mapped[str_255]
    username: Mapped[Optional[str_255]]
    language_code: Mapped[str_255]
    referrer_id: Mapped[Optional[user_fk]]


class Orders(Base, TimestampMixin, TableNameMixin):

    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]


class Products(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Mapped[str]


class OrderProducts(Base, TableNameMixin):

    order_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("products.product_id", ondelete="RESTRICT"), primary_key=True)
    quantity: Mapped[int]


# %% [markdown]
# # Creating a database
# Now you can use SQLAlchemy to create a database.
#
# **Although IT IS NOT RECOMMENDED**, since you would like to track changes in the database, and tracking changes with SQLAlchemy boils down to writing them as raw SQL statements, which is not convenient, i would like to show you how to create your tables with just SQLAlchemy.
#
# We will use Alembic to create tables a little later.

# %%
# You can drop all tables by running the following code:
Base.metadata.drop_all(engine)

# And to create all tables:
#Base.metadata.create_all(engine)

# %% [markdown]
# ## 🎉 Congratulations!  
#
# You have created your first database with **SQLAlchemy**!  
#
# But this is actually **not the correct way** to do it.  
#
# ---
#
# ## ⚡ Using Alembic for Migrations  
#
# We are going to do it with **Alembic**, which is a migration tool for SQLAlchemy.  
#
# As we discussed earlier, we will need to use Alembic to:  
# - Create tables  
# - Track changes in the database  
#
# > **Note:** This history of changes is called **migrations**.  
#
# ---
#
# ## 🔧 Installing Alembic  
#
# In order to use Alembic, you need to install it.  
#
# 👉 Make sure you are using the **virtual environment**, or run this Jupyter Notebook command:  
#
# ```bash
# !pip install alembic      
#

# %%
# %pip install alembic
# %alembic init alembic

# %% [markdown]
# Now there were generated many files in the alembic directory. Please read their tutorial to understand how they work.
#
# In order for the alembic to connect to our database we need to add the following to the env.py file.
#
# ```python
# url = URL.create(
#     drivername="postgresql+psycopg2",  # driver name = postgresql + the library we are using
#     username='testuser',
#     password='testpassword',
#     host='localhost',
#     database='testuser',
#     port=5432
# )
# config.set_main_option('sqlalchemy.url', str(url))
# ```

# %% [markdown]
#
# # Using Environment Variables for Database Configuration
#
# We don't want to store our database connection information directly in the code. Instead, we can use **environment variables**.
#
# ### 1. Create a `.env` file
#
# In the root directory of your project, create a `.env` file and add the following lines:
#
# ```bash
# dotenv
# POSTGRES_USER=testuser
# POSTGRES_PASSWORD=testpassword
# POSTGRES_DB=testuser
# DATABASE_HOST=localhost
# ````
#
# ### 2. Read environment variables in Python
#
# We can use the `environs` library to read the variables from the `.env` file:
#
# ```python
# from environs import Env
#
# env = Env()
# env.read_env()
# ```
#
# ### 3. Replace the URL in `env.py` with environment variables
#
# ```python
# from sqlalchemy.engine.url import URL
#
# url = URL.create(
#     drivername="postgresql+psycopg2",  # driver name = postgresql + psycopg2
#     username=env.str('POSTGRES_USER'),
#     password=env.str('POSTGRES_PASSWORD'),
#     host=env.str('DATABASE_HOST'),
#     database=env.str('POSTGRES_DB'),
#     port=5432
# )
#
# config.set_main_option('sqlalchemy.url', str(url))
# ```
#
# > Here we are replacing the `sqlalchemy.url` from the `alembic.ini` file with the URL we created programmatically.
# > You could still write the connection string directly in `alembic.ini`, but it is **not recommended**.
#
# ### 4. Assign `target_metadata` for Alembic
#
# To allow Alembic to detect your tables, import `Base` from your main file and assign its metadata:
#
# ```python
# from main import Base
#
# # Replace target_metadata = None with:
# target_metadata = Base.metadata
# ```
#
# > Now, you can generate your first migration. Alembic will create Python code in the `alembic/versions` directory.
#
# ```

# %%
# but first, we need to delete the existing tables with SQLAlchemy
Base.metadata.drop_all(engine)

# %% [markdown]
# # Generating Your First Alembic Migration
#
# Now we are ready to generate our first **automatic migration**. Alembic provides a command line interface (CLI) for managing migrations.
#
# ---
#
# ### 1. Create a Migration
#
# To create a new migration script, use the `revision` command:
#
# ```bash
# alembic revision -m "your message"
# ````
#
# Replace `"your message"` with a brief description of the changes the migration will make.
# This will create a new file in the `alembic/versions` directory.
#
# ---
#
# ### 2. Editing a Migration
#
# Open the newly created migration file and locate the `upgrade()` and `downgrade()` functions:
#
# * `upgrade()` is applied when the migration runs.
# * `downgrade()` is applied when a migration is undone.
#
# Example – Adding a new table:
#
# ```python
# def upgrade():
#     op.create_table(
#         'my_new_table',
#         Column('id', Integer, primary_key=True),
#         Column('name', String)
#     )
#
# def downgrade():
#     op.drop_table('my_new_table')
# ```
#
# ---
#
# ### 3. Run Migrations
#
# To apply all pending migrations:
#
# ```bash
# alembic upgrade head
# ```
#
# ---
#
# ### 4. Undo Migrations
#
# * Undo the last migration:
#
# ```bash
# alembic downgrade -1
# ```
#
# * Undo all migrations and return to the start:
#
# ```bash
# alembic downgrade base
# ```
#
# ---
#
# ### 5. Show Current Migration Status
#
# ```bash
# alembic current
# ```
#
# ---
#
# ### 6. Show Migration History
#
# ```bash
# alembic history
# ```
#
# ---
#
# ### 7. Automatic Migrations
#
# Alembic can **autogenerate** migration scripts from the current state of your SQLAlchemy models using the `--autogenerate` option:
#
# ```bash
# alembic revision --autogenerate -m "auto migration"
# ```
#
# > This is a basic introduction to Alembic’s CLI. For more advanced usage, refer to the [official Alembic documentation](https://alembic.sqlalchemy.org/en/latest/).
#
# ```

# %%
# !alembic revision --autogenerate -m "initial migration"

# %%
