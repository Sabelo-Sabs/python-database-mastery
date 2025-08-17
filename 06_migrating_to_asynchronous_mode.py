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
# # Section 6 - Migrating to Asynchronous Mode
# Initializing Connection to Database

# %%
# Import the required modules from SQLAlchemy's async extension
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import URL

# Create a URL object for the PostgreSQL database connection
url = URL.create(
    drivername="postgresql+asyncpg",  # Driver name: PostgreSQL and asyncpg for the async driver
    username='testuser',  # Username for the database
    password='testpassword',  # Password for the database
    host='localhost',  # Host where the database is running
    database='testuser',  # Name of the database
    port=5432  # Port number to connect to the database
).render_as_string(hide_password=False)  # Render the URL as a string. The password is not hidden for demonstration purposes.

# Create an asynchronous database engine
# # echo is set to False to disable SQL logging, making the output cleaner
engine = create_async_engine(url, echo=False)

# Create an asynchronous session maker bound to the engine
session_pool = async_sessionmaker(bind=engine)

# %% [markdown]
# Now let's declare our Users' model again and modify our Repo class to use the async session
#
#

# %%
from sqlalchemy import insert, select, delete, update, bindparam
from typing_extensions import Annotated
from typing import Optional
from sqlalchemy.ext.declarative import declared_attr

import datetime
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import ForeignKey, BIGINT

from sqlalchemy import String
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.functions import func

from sqlalchemy import INTEGER, VARCHAR, DECIMAL
from sqlalchemy.orm import DeclarativeBase


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
    # Next line won't work properly on PostgreSQL, because of the way how it handles TIMESTAMP, unfortunately
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower() + "s"


class User(Base, TimestampMixin, TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(
        BIGINT, primary_key=True, autoincrement=False,
    )
    full_name: Mapped[str_255]
    user_name: Mapped[Optional[str_255]]
    language_code: Mapped[str] = mapped_column(VARCHAR(10))

    # !!! Here I've added ondelete statement, so the referrals won't be deleted
    referrer_id: Mapped[Optional[user_fk]] = mapped_column(
        BIGINT, ForeignKey('users.telegram_id', ondelete='SET NULL')
    )

    orders: Mapped[list['Order']] = relationship(back_populates='user')

    # We can also add __repr__ method to our class to make it more readable on output
    def __repr__(self):
        return f'User: {self.full_name} (ID: {self.telegram_id}). Referrer: {self.referrer_id}'


class Order(Base, TimestampMixin, TableNameMixin):
    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]

    # Notice that we encapsulate our association object class 
    # with quotes to avoid name resolving issues in runtime
    products: Mapped[list['OrderProducts']] = relationship()
    user: Mapped['User'] = relationship(back_populates='orders')


class Product(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(3000))
    price: Mapped[float] = mapped_column(DECIMAL(precision=16, scale=4))
    
    
    # We also add __repr__ method to this class as well to make it more readable on output
    def __repr__(self):
        return f'Product: {self.title} (ID: {self.product_id}). Price: {self.price}'

class OrderProducts(Base, TableNameMixin):
    order_id: Mapped[int] = mapped_column(
        INTEGER, ForeignKey('orders.order_id', ondelete='CASCADE'), primary_key=True,
    )
    product_id: Mapped[int] = mapped_column(
        INTEGER, ForeignKey('products.product_id', ondelete='RESTRICT'), primary_key=True,
    )
    quantity: Mapped[int]

    product: Mapped['Product'] = relationship()


class Repo:
    def __init__(self, session: AsyncSession):
        self.session = session
        
    async def add_user(
            self,
            telegram_id: int,
            full_name: str,
            language_code: str,
            user_name: str = None,
            referrer_id: int = None,
    ):
        stmt = insert(User).values(
            telegram_id=telegram_id,
            full_name=full_name,
            user_name=user_name,
            language_code=language_code,
            referrer_id=referrer_id,
        ).returning(User)
        result = await self.session.execute(stmt)
        await self.session.commit()
        return result.scalars().first()

    async def get_all_users(self):
        stmt = select(User)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def cleanup_users(self):
        stmt = delete(User)
        await self.session.execute(stmt)
        await self.session.commit()



# %% [markdown]
# And let's run these queries:
#
#

# %%
from faker import Faker

fake = Faker()

# !!! You can run the async code in the Jupyter Notebook, but in a usual Python script you'll need to use asyncio.run() to run the async code
async with session_pool() as session:
    repo = Repo(session)
    
    # firstly, let's clean up the database
    await repo.cleanup_users()
    
    # Loop to add 10 new users to the database
    for _ in range(10):
        await repo.add_user(
            telegram_id=fake.pyint(),  # Generates a random integer
            full_name=fake.name(),  # Generates a random full name
            language_code=fake.language_code(),  # Generates a random language code (e.g., "en")
            user_name=fake.user_name(),  # Generates a random username
        )
        
    for user in await repo.get_all_users():
        print(user)
            

# %%
