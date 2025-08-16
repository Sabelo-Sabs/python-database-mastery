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
# # Section 2 - Working with tables
# ## Executing your first queries
# Firstly, lets again initialize the database connection (copy from the previous section):

# %%
from sqlalchemy import create_engine, URL, text
from sqlalchemy.orm import sessionmaker

url = URL.create(
    drivername="postgresql+psycopg2",  # driver name = postgresql + the library we are using (psycopg2)
    username='testuser',
    password='testpassword',
    host='localhost',
    database='testuser',
    port=5432
)

engine = create_engine(url) # skipped echo=True to avoid printing out all the SQL commands
session_pool = sessionmaker(bind=engine)

# %% [markdown]
# Now, to execute any raw SQL query, you would need to import the text function from the sqlalchemy package. This function will allow you to write raw SQL queries in Python. For example, we can execute the query that will create a new table in our database:

# %%

with session_pool() as session:
    query = text("""
    CREATE TABLE users
(
    telegram_id   BIGINT PRIMARY KEY,
    full_name     VARCHAR(255) NOT NULL,
    username      VARCHAR(255),
    language_code VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW(),
    referrer_id   BIGINT,
    FOREIGN KEY (referrer_id)
        REFERENCES users (telegram_id)
        ON DELETE SET NULL
);
    """)
    session.execute(query)
    # and commit the changes
    session.commit()

# %% [markdown]
# Now you can make some other queries to the database. For example, you can select all the data from the users table:

# %%
with session_pool() as session:
    insert_query = text("""
    INSERT INTO users (telegram_id, full_name, username, language_code, referrer_id)
    VALUES (1, 'John Doe', 'johndoe', 'en', NULL),
              (2, 'Jane Doe', 'janedoe', 'en', 1);
    """)
    session.execute(insert_query)
    session.commit()

# %%
with session_pool() as session:

    select_query = text("""
        SELECT * FROM users;
        """)
    result = session.execute(select_query)
    for row in result:
        print(row)

# %% [markdown]
# We can also fetch data results by different methods.
#
# The term "result" in the context of SQLAlchemy and databases generally refers to the data returned from a database query. Depending on the method you use to retrieve the data, the result can be different:
#
# - `execute()`: This is a method on Connection and Engine objects that you use to execute a SQL statement. The result is a ResultProxy object, which is a Python iterable and represents the "cursor" of the database, providing a way to fetch rows.
#
# - `fetchall()`: This is a method on the ResultProxy object, which fetches all rows from the result set and returns them as a list of tuples or, if you're using RowProxy objects (which behave similarly to Python dictionaries), a list of RowProxy objects.
#
# - `fetchone()`: This is another method on the ResultProxy object, which fetches the next row of the result set and returns it as a tuple or a RowProxy object. If there are no more rows, it returns None.
#
# - `first()`: This is another method on the ResultProxy object. It fetches the first row of the result set, and it's essentially equivalent to calling fetchone() immediately after executing the query.
#
# - `scalar()`: This is another method on the ResultProxy object. It fetches the first column of the first row of the result set, and returns it as a Python scalar value. If there are no rows, it returns None.
#
# So in the context of raw SQL queries, the "result" is typically a ResultProxy object that provides several methods to fetch rows from the result set. The rows can be fetched all at once with fetchall(), one at a time with fetchone(), or you can fetch a single value with scalar().
#
# So lets compare the results of them all:

# %%
with session_pool() as session:
    # execute result
    result = session.execute(text("SELECT * FROM users"))
    print(f"execute result: {result}")

    # fetchall result
    result = session.execute(text("SELECT * FROM users")).fetchall()
    print(f"fetchall result: {result}")

    # fetchone result
    result = session.execute(text("SELECT * FROM users")).fetchone()
    print(f"fetchone result (one row): {result}")

    # first result
    result = session.execute(text("SELECT * FROM users")).first()
    print(f"first result (one row): {result}")

    # scalar result
    result = session.execute(text("SELECT username FROM users WHERE telegram_id = :telegram_id"), {"telegram_id": 1}).scalar()
    print(f"scalar result username: {result}")

    # scalar one or none result
    result = session.execute(text("SELECT username FROM users WHERE telegram_id = :telegram_id"), {"telegram_id": 12345}).scalar_one_or_none()
    print(f"scalar one or none result username: {result}")

# %% [markdown]
# # Creating tables with SQLAlchemy
# ## ORM
# So you know how to create tables in SQL, now you can use SQLAlchemy to create them. If you are not familiar with Object oriented programming (OOP) at this point, you have to learn it first, since we will be using Object Relational Mapping (ORM) to create tables.
#
# SQLAlchemy is a powerful Object Relational Mapper (ORM) for Python, which allows you to interact with your database using Python objects and classes rather than writing raw SQL queries. ORM provides a high-level, abstraction layer on top of SQL, making it easy to work with databases in a more Pythonic way, while still leveraging the full power of SQL.
#
# Basically, you will have access to your database tables as to python objects, and to tables' columns as to attributes of these objects.
#
# The central idea behind ORM is to map the database tables to Python classes, and the table rows to class instances (objects). This way, you'll be able to perform database operations using object-oriented programming concepts, such as inheritance, associations, and encapsulation.
#
# ## Disclaimer
# This is a very short tutorial to introduce you to SQLAlchemy. If you want to learn more, you can read the official documentation: https://docs.sqlalchemy.org/en/20/tutorial/index.html

# %% [markdown]
# # Creating a table
# To map our Python classes to database tables, we'll use SQLAlchemy's Declarative system.
#
# To start creating tables, you will need a specific SQLAlchemy base class to inherit from, so SQLAlchemy will understand how to map the results of your queries to Python objects.
#
# This class is called declarative base and is created like this:

# %%
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass


# %% [markdown]
# Now you can start creating tables as Python classes.
#
# Do you remember how we created the users table in SQL?
#
#
# ```sql
#
# CREATE TABLE users
# (
#     telegram_id   BIGINT PRIMARY KEY,
#     full_name     VARCHAR(255) NOT NULL,
#     username      VARCHAR(255),
#     language_code VARCHAR(255) NOT NULL,
#     created_at    TIMESTAMP DEFAULT NOW(),
#     referrer_id   BIGINT,
#     FOREIGN KEY (referrer_id)
#         REFERENCES users (telegram_id)
#         ON DELETE SET NULL
# );
#
#
# ```

# %% [markdown]
# Let's create a class to represent this table:
#
# 1. To create a table in SQLAlchemy, you need to create a class that inherits from the declarative base.
# 2. In order to create columns in the table, you need to create new attributes and assign them the `Column` class. Since the 2.0 you can use `mapped_column` function to create columns and Mapped type annotations to define types of columns.
# 3. To use SQL data types you have to import specific objects from the `sqlalchemy` module. They usually have the same names. Examples: `BIGINT`, `VARCHAR`, `TIMESTAMP`.
# 4. To create a primary key, you need to pass the `primary_key` argument to the column.
# 5. To create a not null constraint, you need to pass the `nullable` argument to the column.
# 6. To create a default value, you need to pass the `server_default` argument to the column.
# 7. To create a foreign key, you need to pass the `ForeignKey` argument to the column and fill its arguments.
# 8. To use SQL expressions from SQLAlchemy you can use `sqlalchemy.sql.expression` module. For example, you can use `null()` or `false()` to create a default values `NULL` or `FALSE` for a column.
# 9. To use functions from SQLAlchemy you can use `sqlalchemy.func` module. For example, you can use `func.now()` to create a default `NOW()` value for a column.
# 10. To give a name to a table you must always specify the **__tablename__** attribute.
#
# To create tables in 2.0 syntax you will need to use **mapped_column** for creating columns.

# %%
from sqlalchemy import INTEGER
from sqlalchemy.orm import DeclarativeBase


# Creating a base class
class Base(DeclarativeBase):
    pass


# %% [markdown]
# After that, we declare the columns that make up each table.
#
# These columns are declared using a special typing annotation called `Mapped`. The Python datatype associated with each Mapped annotation determines the corresponding SQL datatype, e.g., int for `INTEGER` or str for `VARCHAR`. Nullability is based on whether or not the `Optional[]` type modifier is used, but can also be specified explicitly using the nullable parameter.
#
# The `mapped_column()` directive is applied to column-based attributes, allowing SQLAlchemy to handle column properties, such as server `defaults`, primary key constraints, and foreign key constraints. Every ORM mapped class must have at **least one column declared as a primary key**. In our example, User.telegram_id is marked as the primary key by setting `primary_key=True`.
#
#
# ```python
#
# class User(Base):
#     __tablename__ = "users"
#
#     telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True)
#     full_name: Mapped[str] = mapped_column(VARCHAR(255))
#     username: Mapped[Optional[str]] = mapped_column(VARCHAR(255), nullable=True)
#     language_code: Mapped[str] = mapped_column(VARCHAR(255))
#     created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
#     referrer_id: Mapped[Optional[int]] = mapped_column(BIGINT, ForeignKey('users.telegram_id', ondelete='SET NULL'))
#
# ```
#
#
# Relationships between tables are defined using the `relationship()` construct, which creates links between ORM classes. Instead of direct column mappings, relationship() provides associations between two ORM classes. In this lesson’s example, there aren't any relationships present, apart from ForeignKey itself, but you can review a more detailed tutorial on working with ORM related objects in the SQLAlchemy Unified Tutorial.
#
# But we are not finished yet.
#
# You can also prepare a `Mixin` class, if you want to reuse definitions of columns in other tables. https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html

# %%
from datetime import datetime
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import ForeignKey, BIGINT, String

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.functions import func


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


# %% [markdown]
# Now your Users class will look like this:

# %%
class Users(Base, TimestampMixin):
    __tablename__ = "users"

    telegram_id: Mapped[int] = mapped_column(BIGINT, nullable=False, primary_key=True)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str] = mapped_column(String(255), nullable=True)
    language_code: Mapped[str] = mapped_column(String(255), nullable=False)
    referrer_id: Mapped[int] = mapped_column(BIGINT, ForeignKey('users.telegram_id', ondelete='SET NULL'))


# %% [markdown]
# **BUT!** You can do even better.
#
# Sometimes you will want to reuse foreign keys or other columns, but give them different names. In this case, you can use `Annotated` class:

# %%
from typing_extensions import Annotated
from typing import Optional

# Users ForeignKey
user_fk = Annotated[
    int, mapped_column(BIGINT, ForeignKey("users.telegram_id", ondelete="CASCADE"))
]

# integer primary key
int_pk = Annotated[int, mapped_column(INTEGER, primary_key=True)]

# string column with length 255
str_255 = Annotated[str, mapped_column(String(255))]

# So now your Users class will look like this:
class Users(Base, TimestampMixin):
    __tablename__ = "users"

    telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=False)
    full_name: Mapped[str_255]
    username: Mapped[Optional[str_255]]
    language_code: Mapped[str_255]
    referrer_id: Mapped[Optional[user_fk]]


# %% [markdown]
# You can also create a Mixin class for generating tables names from class names.

# %%
from sqlalchemy.ext.declarative import declared_attr

class TableNameMixin:

    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower() + "s"


# %% [markdown]
# Do you see how clean and readable the code is?
#
# Let's now make some more tables from the previous tutorials.
#
# **Note:** You can use autoincrement argument to create a SERIAL type column. If you don't want to use SERIAL in primary key, you will need to specify `autoincrement=False` in the column definition.
#
# ```sql
#
# CREATE TABLE orders
# (
#     order_id   SERIAL PRIMARY KEY,
#     user_id    BIGINT NOT NULL,
#     created_at TIMESTAMP DEFAULT NOW(),
#     FOREIGN KEY (user_id)
#         REFERENCES users (telegram_id)
#         ON DELETE CASCADE
# );
#
# ```
#

# %%
class Orders(Base, TimestampMixin, TableNameMixin):

    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]


# %% [markdown]
# ```sql
#
# CREATE TABLE products
# (
#     product_id  SERIAL PRIMARY KEY,
#     title       VARCHAR(255) NOT NULL,
#     description TEXT,
#     created_at  TIMESTAMP DEFAULT NOW()
# );
# ```

# %%
class Products(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Mapped[str]


# %% [markdown]
# ```sql
#
# CREATE TABLE order_products
# (
#     order_id   INTEGER NOT NULL,
#     product_id INTEGER NOT NULL,
#     quantity   INTEGER NOT NULL,
#     FOREIGN KEY (order_id)
#         REFERENCES orders (order_id)
#         ON DELETE CASCADE,
#     FOREIGN KEY (product_id)
#         REFERENCES products (product_id)
#         ON DELETE RESTRICT
# );
# ```

# %%
class OrderProducts(Base, TableNameMixin):

    order_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("products.product_id", ondelete="RESTRICT"), primary_key=True)
    quantity: Mapped[int]

# %% [markdown]
# Congratulations! You have created your first tables in SQLAlchemy! 🎉
#
# Well... almost. We just defined the tables, but we didn't create them in the database yet. See you in the next section!
