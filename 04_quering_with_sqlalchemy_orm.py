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
# # Section 4 - Querying with SQLAlchemy ORM
# ## Initializing Connection to Database
# Firstly, lets once again initialize the database connection (copy from the previous section)

# %%
from environs import Env
from sqlalchemy import create_engine, URL
from sqlalchemy.orm import sessionmaker

env = Env()
env.read_env('.env')

url = URL.create(
    drivername="postgresql+psycopg2",
    username=env.str('POSTGRES_USER'),
    password=env.str('POSTGRES_PASSWORD'),
    host=env.str('DATABASE_HOST'),
    port=5432,
    database=env.str('POSTGRES_DB'),
).render_as_string(hide_password=False)

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
from sqlalchemy import ForeignKey, BIGINT

from sqlalchemy import String
from sqlalchemy.orm import Mapped
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
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class TableNameMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower() + "s"


class User(Base, TimestampMixin, TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=False)
    full_name: Mapped[str_255]
    user_name: Mapped[Optional[str_255]]
    language_code: Mapped[str_255]
    referrer_id: Mapped[Optional[user_fk]]


class Order(Base, TimestampMixin, TableNameMixin):
    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]


class Product(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(3000))
    price: Mapped[float] = mapped_column(DECIMAL(precision=16, scale=4))


class OrderProducts(Base, TableNameMixin):
    order_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True)
    product_id: Mapped[int] = mapped_column(INTEGER, ForeignKey("products.product_id", ondelete="RESTRICT"), primary_key=True)
    quantity: Mapped[int]


# %%
# You can drop all tables by running the following code:
Base.metadata.drop_all(engine)

# And to create all tables:
Base.metadata.create_all(engine)

# %% [markdown]
# # Repository
# Repository is a class that stores and manages the database interaction
#
# **Insert statement**

# %%
from sqlalchemy import insert, select
from sqlalchemy.orm import Session


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def add_user(
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
        )
        self.session.execute(stmt)
        self.session.commit()


# %% [markdown]
# ## Testing the Insert ORM statement
#

# %%
with session_pool() as session:
    repo = Repo(session)
    repo.add_user(
        telegram_id=1,
        full_name='John Doe',
        user_name='johnny',
        language_code='en',
    )

# %% [markdown]
# ## Simple and advanced Select statements
#

# %%
from typing import List

from sqlalchemy import or_


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def get_user_by_id(self, telegram_id: int) -> User:
        # Notice that you should pass the comparison-like arguments 
        # to WHERE statement, as you can see below, we are using 
        # `User.telegram_id == telegram_id` instead of 
        # `User.telegram_id = telegram_id`
        stmt = select(User).where(User.telegram_id == telegram_id)
        result = self.session.execute(stmt)
        # After we get the result of our statement execution, we need to
        # define HOW we want to get the data. In most cases you want to 
        # get the object(s) or only one value. To retrieve the object 
        # itself, we call the `scalars` method of our result. Then we 
        # have to define HOW MANY records you want to get. It can be 
        # `first` object, `one` (raises an error if there are not 
        # exactly one row retrieved)) / `one_or_none` and so on.
        return result.scalars().first()

    def get_all_users_simple(self) -> List[User]:
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def get_all_users_advanced(self) -> List[User]:
        stmt = select(
            User,
        ).where(
            # OR clauses' syntax is explicit-only, unlike the AND clause.
            # You can pass each argument of OR statement as arguments to 
            # `sqlalchemy.or_` function, like on the example below
            or_(
                User.language_code == 'en',
                User.language_code == 'uk',
            ),
            # Each argument that you pass to `where` method of the Select object 
            # considered as an argument of AND statement
            User.user_name.ilike('%john%'),
        ).order_by(
            User.created_at.desc(),
        ).limit(
            10,
        ).having(
            User.telegram_id > 0,
        ).group_by(
            User.telegram_id,
        )
        result = self.session.execute(stmt)
        return result.scalars().all()

    def get_user_language(self, telegram_id: int) -> str:
        stmt = select(User.language_code).where(User.telegram_id == telegram_id)
        result = self.session.execute(stmt)
        return result.scalar()


# %% [markdown]
# ## Testing the Select ORM statements

# %%
with session_pool() as session:
    repo = Repo(session)
    user = repo.get_user_by_id(1)
    print(
        f'User: {user.telegram_id} '
        f'Full name: {user.full_name} '
        f'Username: {user.user_name} '
        f'Language code: {user.language_code}'
    )
    all_users = repo.get_all_users_simple()
    print(all_users)
    users = repo.get_all_users_advanced()
    print(users)
    user_language = repo.get_user_language(1)
    print(user_language)

# %% [markdown]
# So, as you can see, the SQLAlchemy syntax is quite similar with raw SQL.
#
#

# %% [markdown]
# ## Combining Insert, Select and Update in a Single Query
#

# %%
from sqlalchemy.dialects.postgresql import insert


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def add_user(
        self,
        telegram_id: int,
        full_name: str,
        language_code: str,
        user_name: str = None,
        referrer_id: int = None,
    ) -> User:
        # !!!IMPORTANT!!! Notice the import statement at the beginning of this
        # cell. `.returning(...)` and `.on_conflict_do_nothing()` (as well as
        # `.on_conflict_do_update(...)`) methods aren't accessible by using basic
        # `sqlalchemy.insert` constructor. These are parts of PostgreSQL dialect.
        # What are we trying to achieve? We want to INSERT user every time and
        # SELECT it if no conflict occurs (on the DB side). And, if there is a
        # conflict, do UPDATE and only then SELECT updated row.
        insert_stmt = insert(
            User,
        ).values(
            telegram_id=telegram_id,
            full_name=full_name,
            language_code=language_code,
            user_name=user_name,
            referrer_id=referrer_id,
        # Here we are using new method which represents RETURNING 
        # instruction in raw SQL (particularly PostgreSQL syntax)
        ).returning(
            User,
        # Also, another method which uses raw PostgreSQL instuction,
        # such as ON CONFLICT DO ...
        # In that case, we are using ON CONFLICT DO UPDATE, but 
        # ON CONFLICT DO NOTHING is also achievable by using 
        # `.on_conflict_do_nothing()` method.
        ).on_conflict_do_update(
            # `index_elements` argument is for array of entities used
            # in order to distinguish records from each other.
            index_elements=[User.telegram_id],
            # `set_` argument (we add underscore at the end because 
            # `set` is reserved name in python, we can't use it as 
            # a key) used to define which columns you wish to update
            # in case of conflict. Almost identical to use of `.values()` method.
            set_=dict(
                user_name=user_name,
                full_name=full_name,
            ),
        )
        # And here we are declaring that we want to SELECT 
        # the entity from our INSERT statement.
        stmt = select(User).from_statement(insert_stmt)
        # Also, here is another way to execute your statement and retrieve data.
        # You can use `session.scalars(stmt)` instead of `session.execute(stmt).scalars()`
        result = self.session.scalars(stmt).first()
        self.session.commit()
        return result


# %% [markdown]
# ## Testing combined query
#

# %%
with session_pool() as session:
    repo = Repo(session)
    # You can notice that this code never crashes on conflict.
    user = repo.add_user(
        telegram_id=2,
        full_name='Juan Perez',
        user_name='juanpe',
        language_code='es',
    )
    print(
        f'User: {user.telegram_id} '
        f'Full name: {user.full_name} '
        f'Username: {user.user_name} '
        f'Language code: {user.language_code}'
    )

# %% [markdown]
# Seeding Initial Data to Database
# In the next few cells we would use library called Faker to generate data to fill our database.
#
# First of all, lets install the library

# %%
# %pip install faker


# %% [markdown]
# Now, lets generate our fake data.
#
#

# %%
import random

from faker import Faker


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def add_user(
        self,
        telegram_id: int,
        full_name: str,
        language_code: str,
        user_name: str = None,
        referrer_id: int = None,
    ) -> User:
        insert_stmt = insert(
            User,
        ).values(
            telegram_id=telegram_id,
            full_name=full_name,
            language_code=language_code,
            user_name=user_name,
            referrer_id=referrer_id,
        ).on_conflict_do_update(
            index_elements=[User.telegram_id],
            set_=dict(
                user_name=user_name,
                full_name=full_name,
            ),
        ).returning(User)
        stmt = select(User).from_statement(insert_stmt)
        result = self.session.scalars(stmt)
        self.session.commit()
        return result.first()

    # Lets define all necessary methods to create records in multiple tables

    def add_order(self, user_id: int) -> Order:
        stmt = select(Order).from_statement(
            insert(Order).values(user_id=user_id).returning(Order),
        )
        result = self.session.scalars(stmt)
        self.session.commit()
        return result.first()

    def add_product(self, title: str, description: str, price: int) -> Product:
        stmt = select(Product).from_statement(
            insert(Product)
            .values(title=title, description=description, price=price)
            .returning(Product),
        )
        result = self.session.scalars(stmt)
        self.session.commit()
        return result.first()

    def add_order_product(self, order_id: int, product_id: int, quantity: int):
        stmt = (
            insert(OrderProducts)
            .values(order_id=order_id, product_id=product_id, quantity=quantity)
        )
        self.session.execute(stmt)
        self.session.commit()


def seed_fake_data(repo: Repo):
    # Here we can define something like randomizing key.
    # If we pass same seed every time we would get same 
    # sequence of random data.
    Faker.seed(0)
    fake = Faker()
    # Lets predefine our arrays of fake entities so we 
    # can reference them to create relationships and(or)
    # to give referrer_id to some users and so on.
    users = []
    orders = []
    products = []

    # add users
    for _ in range(10):
        referrer_id = None if not users else users[-1].telegram_id
        user = repo.add_user(
            telegram_id=fake.pyint(),
            full_name=fake.name(),
            language_code=fake.language_code(),
            user_name=fake.user_name(),
            referrer_id=referrer_id,
        )
        users.append(user)

    # add orders
    for _ in range(10):
        order = repo.add_order(
            user_id=random.choice(users).telegram_id,
        )
        orders.append(order)

    # add products
    for _ in range(10):
        product = repo.add_product(
            title=fake.word(),
            description=fake.sentence(),
            price=fake.pyint(),
        )
        products.append(product)

    # add products to orders
    for order in orders:
        # Here we use `sample` function to get list of 3 unique products
        for product in random.sample(products, 3):
            repo.add_order_product(
                order_id=order.order_id,
                product_id=product.product_id,
                quantity=fake.pyint(),
            )


with session_pool() as session:
    repo = Repo(session)
    seed_fake_data(repo)

# %% [markdown]
# # Establishing relationships in Tables
# Defining relationships in SQLAlchemy ORM is slightly easy process of configuring attributes in your mapping classes and loading strategies in your queries.
#
# We are recommending you to visit SQLAlchemy documentation. Particularly, check this page to learn about different kinds of relationship patterns, such as one-to-one, many-to-one, one-to-many, many-to-many.
#
# Since this is a Jupyter Notebook environment, we are going to redefine our mapping classes, but in real world you can just add attributes and thats it. Lets proceed.

# %%
from sqlalchemy.orm import relationship


# I'm doing it only to reset SQLAlchemy MetaData. Not necessary in real world.
class Base(DeclarativeBase):
    pass


class Order(Base, TimestampMixin, TableNameMixin):
    order_id: Mapped[int_pk]
    user_id: Mapped[user_fk]

    # Notice that we encapsulate our association object class 
    # with quotes to avoid name resolving issues in runtime
    products: Mapped[list['OrderProducts']] = relationship()
    user: Mapped['User'] = relationship(back_populates='orders')


class OrderProducts(Base, TableNameMixin):
    order_id: Mapped[int] = mapped_column(
        INTEGER, ForeignKey('orders.order_id', ondelete='CASCADE'), primary_key=True,        
    )
    product_id: Mapped[int] = mapped_column(
        INTEGER, ForeignKey('products.product_id', ondelete='RESTRICT'), primary_key=True,
    )
    quantity: Mapped[int]

    product: Mapped['Product'] = relationship()


class User(Base, TimestampMixin, TableNameMixin):
    telegram_id: Mapped[int] = mapped_column(
        BIGINT, primary_key=True, autoincrement=False,
    )
    full_name: Mapped[str_255]
    user_name: Mapped[Optional[str_255]]
    language_code: Mapped[str] = mapped_column(VARCHAR(10))
    referrer_id: Mapped[Optional[user_fk]]

    orders: Mapped[list['Order']] = relationship(back_populates='user')


class Product(Base, TimestampMixin, TableNameMixin):
    product_id: Mapped[int_pk]
    title: Mapped[str_255]
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(3000))
    price: Mapped[float] = mapped_column(DECIMAL(precision=16, scale=4))


# %% [markdown]
# ## ORM JOIN Queries (INNER, OUTER)
# Lets create a method to get all invited (referrer_id IS NOT NULL) users

# %%
from sqlalchemy.orm import aliased


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def select_all_invited_users(self):
        ParentUser = aliased(User)
        ReferralUser = aliased(User)

        stmt = (
            select(
                ParentUser.full_name.label('parent_name'),
                ReferralUser.full_name.label('referral_name'),
            ).join(
                ReferralUser, ReferralUser.referrer_id == ParentUser.telegram_id,
            )
        )
        result = self.session.execute(stmt)
        return result.all()


# %% [markdown]
# ## Test it!
#

# %%
with session_pool() as session:
    repo = Repo(session)
    for row in repo.select_all_invited_users():
        print(f'Parent: {row.parent_name}, Referral: {row.referral_name}')


# %% [markdown]
# # Advanced Select Queries with Joins with SQLAlchemy ORM
# Finally, we are going to learn how to use SQLAlchemy relationships!
#
# First of all, lets see if we can use them already

# %%
class Repo:
    def __init__(self, session: Session):
        self.session = session

    def get_all_users(self) -> List[User]:
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()


with session_pool() as session:
    repo = Repo(session)
    for user in repo.get_all_users():
        print(f'User: {user.full_name} ({user.telegram_id})')
        for order in user.orders:
            print(f'\tOrder: {order.order_id}')
            for product_association in order.products:
                print(f'\t\tProduct: {product_association.product.title}')


# %% [markdown]
# From the output of cell above, you can see that it works, but it execute a lot of statements, so no JOINs were used at all. It is very ineffective way.
#
# Lets do the real statement with joins and SQLAlchemy relationships!

# %%
class Repo:
    def __init__(self, session: Session):
        self.session = session

    def get_all_user_orders_user_full(self, telegram_id: int):
        stmt = (
            select(Order, User).join(User.orders).where(User.telegram_id == telegram_id)
        )
        # NOTICE: Since we are joining two tables, we won't use `.scalars()` method.
        # Usually we want to use scalars if we are joining multiple tables or 
        # when you use `.label()` method to retrieve some specific column etc.
        result = self.session.execute(stmt)
        return result.all()

    def get_all_user_orders_user_only_user_name(self, telegram_id: int):
        stmt = (
            select(Order, User.user_name).join(User.orders).where(User.telegram_id == telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()


with session_pool() as session:
    repo = Repo(session)
    user_orders = repo.get_all_user_orders_user_full(telegram_id=4104)
    # You have two ways of accessing retrieved data, first is like below:
    for order, user in user_orders:
        print(f'Order: {order.order_id} - {user.full_name}')
    print('=============')
    # Second is like next:
    for row in user_orders:
        print(f'Order: {row.Order.order_id} - {row.User.full_name}')
    print('=============')
    # In the next two examples you can see how to access your data when
    # you didn't specified only full tables
    user_orders = repo.get_all_user_orders_user_only_user_name(telegram_id=4104)
    for order, user_name in user_orders:
        print(f'Order: {order.order_id} - {user_name}')
    print('=============')
    for row in user_orders:
        # As you can see, if we specified column instead of full table, 
        # we can access it directly from row by using the name of column
        print(f'Order: {row.Order.order_id} - {row.user_name}')


# %% [markdown]
# As you can see, we have only one query executed and all orders of the specified user given.
#
# Lets do something more advanced!

# %%
class Repo:
    def __init__(self, session: Session):
        self.session = session

    def get_all_user_orders_relationships(self, telegram_id: int):
        stmt = (
            select(Product, Order, User.user_name, OrderProducts.quantity)
            .join(User.orders)
            .join(Order.products)
            .join(Product)
            .where(User.telegram_id == telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()

    def get_all_user_orders_no_relationships(self, telegram_id: int):
        stmt = (
            select(Product, Order, User.user_name, OrderProducts.quantity)
            .join(OrderProducts)
            .join(Order)
            .join(User)
            .select_from(Product)
            .where(User.telegram_id == telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()


with session_pool() as session:
    repo = Repo(session)

    user_orders1 = repo.get_all_user_orders_relationships(telegram_id=4104)
    user_orders2 = repo.get_all_user_orders_no_relationships(telegram_id=4104)

    # Shows that both query results are identical
    assert user_orders1 == user_orders2

    for product, order, user_name, quantity in user_orders1:
        print(
            f'#{product.product_id} Product: {product.title} (x {quantity}) Order: {order.order_id}: {user_name}'
        )

# %% [markdown]
# So, with only one query we can access data from multiple tables in a pretty simple way with SQLAlchemy.
#
# Play around with JOINs by yourself, go to SQLAlchemy docs to see their examples, get your experience.

# %% [markdown]
# # Aggregated Queries using SQLAlchemy
# So, SQLAlchemy allows us to use aggregation SQL functions like SUM, COUNT, MIN/MAX/AVG and so on.

# %%
from sqlalchemy import func


class Repo:
    def __init__(self, session: Session):
        self.session = session

    def get_user_total_number_of_orders(self, telegram_id: int):
        stmt = (
            # All SQL aggregation functions are accessible with `sqlalchemy.func` module
            select(func.count(Order.order_id)).where(Order.user_id == telegram_id)
        )
        # As you can see, if we want to get only one value with our query,
        # we can just use `.scalar(stmt)` method of our Session.
        result = self.session.scalar(stmt)
        return result

    def get_total_number_of_orders_by_user(self):
        stmt = (
            select(func.count(Order.order_id), User.telegram_id)
            .join(User)
            .group_by(User.telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()

    def get_total_number_of_orders_by_user_with_labels(self):
        stmt = (
            select(func.count(Order.order_id).label('quantity'), User.full_name.label('name'))
            .join(User)
            .group_by(User.telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()

    def get_count_of_products_by_user(self):
        stmt = (
            select(func.sum(OrderProducts.quantity).label('quantity'), User.full_name.label('name'))
            .join(Order, Order.order_id == OrderProducts.order_id)
            .join(User)
            .group_by(User.telegram_id)
        )
        result = self.session.execute(stmt)
        return result.all()

    def get_count_of_products_greater_than_x_by_user(self, greater_than: int):
        stmt = (
            select(func.sum(OrderProducts.quantity).label('quantity'), User.full_name.label('name'))
            .join(Order, Order.order_id == OrderProducts.order_id)
            .join(User)
            .group_by(User.telegram_id)
            .having(func.sum(OrderProducts.quantity) > greater_than)
        )
        result = self.session.execute(stmt)
        return result.all()


with session_pool() as session:
    repo = Repo(session)
    user_telegram_id = 3909
    user_total_number_of_orders = repo.get_user_total_number_of_orders(telegram_id=user_telegram_id)
    print(f'[User: {user_telegram_id}] total number of orders: {user_total_number_of_orders}')
    print('===========')
    for orders_count, telegram_id in repo.get_total_number_of_orders_by_user():
        print(f'Total number of orders: {orders_count} by {telegram_id}')
    print('===========')
    for row in repo.get_total_number_of_orders_by_user_with_labels():
        print(f'Total number of orders: {row.quantity} by {row.name}')
    print('===========')
    for products_count, name in repo.get_count_of_products_by_user():
        print(f'Total number of products: {products_count} by {name}')
    print('===========')
    for products_count, name in repo.get_count_of_products_greater_than_x_by_user(20_000):
        print(f'Total number of products: {products_count} by {name}')

# %%
