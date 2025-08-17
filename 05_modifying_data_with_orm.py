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
# # Section 5 - Modifying Data with ORM
# Initializing Connection to Database
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
).render_as_string(hide_password=False)

# I've set echo False so we have a cleaner output
engine = create_engine(url, echo=False)
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


# %%
# You can drop all tables by running the following code:
Base.metadata.drop_all(engine)

# And to create all tables:
Base.metadata.create_all(engine)

# %% [markdown]
# # Repository
# Repository is a class that stores and manages the database interaction
#
# # Adding Repo methods
# Let's first clean up the users and test our new queries on fresh data.

# %%
from sqlalchemy import insert, select, delete
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
        ).returning(User)
        
        result = self.session.execute(stmt)
        self.session.commit()
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()

    # Here is how we can delete all users from the database
    def cleanup_users(self):
        stmt = delete(User)
        self.session.execute(stmt)
        self.session.commit()


# %% [markdown]
# # Run Repo methods
# Let's fill out the users with faker again. Make sure you have faker library installed.

# %%
# %pip install faker

# %%
# In case you've skipped the alembic section, you'll need to create the tables, you can do it by uncommenting and running the following lines
# Base.metadata.drop_all(engine)
# Base.metadata.create_all(engine)

# %%
# Import the Faker library to generate fake data
from faker import Faker

# Initialize the Faker object
fake = Faker()

# Create a session using a session pool for database operations
with session_pool() as session:
    # Initialize the repository to perform operations on the database
    repo = Repo(session)
    
    # Cleanup any existing users in the database before adding new ones
    repo.cleanup_users()
    
    # Initialize an empty list to keep track of generated users
    users = []
    
    # Loop to add 10 new users to the database
    for _ in range(10):
        # Add a new user to the database using the add_user method of the Repo class
        # Each user will have a random Telegram ID, full name, language code, and username
        user = repo.add_user(
            telegram_id=fake.pyint(),  # Generates a random integer
            full_name=fake.name(),  # Generates a random full name
            language_code=fake.language_code(),  # Generates a random language code (e.g., "en")
            user_name=fake.user_name(),  # Generates a random username
        )
        

# %% [markdown]
# Let's check if we have users in the database

# %%
with session_pool() as session:
    repo = Repo(session)
    for user in repo.get_all_users():
        print(user)

# %% [markdown]
# # Lesson 1: Update the users' referrers
# Let's update our Repo class to include the method

# %%
from sqlalchemy import insert, select, delete, update
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
        ).returning(User)
        
        result = self.session.execute(stmt)
        self.session.commit()
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def cleanup_users(self):
        stmt = delete(User)
        self.session.execute(stmt)
        self.session.commit()

    def set_new_referrer(self, user_id: int, referrer_id: int):
        # Create an 'update' statement for the User table
        # SQLAlchemy's 'update' function is used to update records
        stmt = (
            update(User)  # Specify that you're updating the User table
            .where(User.telegram_id == user_id)  # Specify which row(s) to update using the 'where' clause
            .values(referrer_id=referrer_id)  # Define the new value(s) using the 'values' method
            # The '.returning' clause specifies what should be returned after the update
        ).returning(User)
        
        # Execute the statement and store the result
        result = self.session.execute(stmt)
        
        # Commit the transaction to make the update permanent
        self.session.commit()
        
        # Retrieve the first updated object from the Result object
        # '.scalars().first()' fetches the first updated record as an object
        return result.scalars().first()


# %% [markdown]
# # Testing the Update method
#

# %%
with session_pool() as session:
    repo = Repo(session)
    # If you already know the user id - you can use it, otherwise you can get it from the database
    users = repo.get_all_users()
    for user in users:
        print(user)

# %% [markdown]
# Now we can use the select some of the users and set some referrers

# %%
with session_pool() as session:
    repo = Repo(session)
    new_user = repo.set_new_referrer(user_id=users[0].telegram_id, referrer_id=users[1].telegram_id)
    print(new_user)
    new_another_user = repo.set_new_referrer(user_id=users[2].telegram_id, referrer_id=users[3].telegram_id)
    print(new_another_user)

# %% [markdown]
# # Lesson 2: Delete the user by id
# We'll have to update our Repo class again

# %%
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
        ).returning(User)
        
        result = self.session.execute(stmt)
        self.session.commit()
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def cleanup_users(self):
        stmt = delete(User)
        self.session.execute(stmt)
        self.session.commit()

    def set_new_referrer(self, user_id: int, referrer_id: int):
        stmt = (
            update(User)  # Specify that you're updating the User table
            .where(User.telegram_id == user_id)  # Specify which row(s) to update using the 'where' clause
            .values(referrer_id=referrer_id)  # Define the new value(s) using the 'values' method
        ).returning(User)
        result = self.session.execute(stmt)
        self.session.commit()
        
        return result.scalars().first()


    def delete_user_by_id(self, user_id: int):
        # Create a 'delete' statement for the User table
        # SQLAlchemy's 'delete' function is used to delete records
        stmt = (
            delete(User)  # Specify that you're deleting from the User table
            # Use the 'where' clause to specify which row(s) should be deleted
            # In this case, delete where the 'telegram_id' matches 'user_id'
            .where(User.telegram_id == user_id)
        )
        # Execute the SQL statement
        # 'session.execute' runs the SQL statement, but changes are not committed yet
        self.session.execute(stmt)
        
        # Commit the transaction to make the deletion permanent
        self.session.commit()

# %% [markdown]
# # Testing the Delete method

# %%
with session_pool() as session:
    repo = Repo(session)
    # Retrieve all users from the database and print them
    for user in repo.get_all_users():
        print(user)
    
    # Delete the first three users in the list
    for user in users[:3]:
        # Call the delete_user_by_id method from the Repo class to delete each user by their telegram_id 
        repo.delete_user_by_id(user.telegram_id)
        
    # Print the list of all users after deletion to confirm
    print("After deletion:")
    for user in repo.get_all_users():
        print(user)

# %%
# let's count the number of users.
# !!! The number should be 7. 
# If there are less, then you'll have to add ondelete='SET NULL' to the referrer_id column of the User table
with session_pool() as session:
    repo = Repo(session)
    users = repo.get_all_users()
    print(len(users))

# %% [markdown]
# ## Bulk insert users
# Now, lets update our repo with bulk insert method

# %%
from sqlalchemy import insert, select, delete, update, bindparam
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
        ).returning(User)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.scalars().first()

    def get_all_users(self):
        stmt = select(User)
        result = self.session.execute(stmt)
        return result.scalars().all()

    def cleanup_users(self):
        stmt = delete(User)
        self.session.execute(stmt)
        self.session.commit()

    def set_new_referrer(self, user_id: int, referrer_id: int):
        stmt = (
            update(User)
            .where(User.telegram_id == user_id)
            .values(referrer_id=referrer_id)
            # We can also return the updated user
        ).returning(User)
        result = self.session.execute(stmt)
        self.session.commit()
        return result.scalars().first()

    def delete_user_by_id(self, user_id: int):
        stmt = delete(User).where(User.telegram_id == user_id)
        self.session.execute(stmt)
        self.session.commit()

    def create_new_order_for_user(self, user_id):
        # Create an 'insert' statement to add a new order linked to a user
        new_order = insert(Order).values(
            user_id=user_id  # Set the 'user_id' column value
        ).returning(Order.order_id)  # Return the new order_id generated
        
        # Execute the insert statement and commit the changes
        result = self.session.execute(new_order)
        self.session.commit()
        
        # Retrieve and return the order_id of the newly created order
        return result.scalar()

    def add_bulk_products(self, products: list[dict]):
        # Create an 'insert' statement for adding multiple products at once
        stmt = insert(Product).values(
            title=bindparam('title'),  # Use bindparam for batch insertion
            description=bindparam('description'),
            price=bindparam('price')
        ).returning(Product)
        
        # Execute the insert statement with the list of product dictionaries
        result = self.session.execute(stmt, products)
        self.session.commit()
        
        # Return all newly inserted products
        return result.scalars().all()

        
    def bulk_insert_products_into_order_id(self, order_id: int, products: list[dict]):
        # Create an 'insert' statement for adding multiple products to an order
        stmt = insert(OrderProducts).values(
            order_id=order_id,  # Set the order_id
            product_id=bindparam('product_id'),  # Use bindparam for batch insertion
            quantity=bindparam('quantity')
        )
        
        # Execute the insert statement with the list of product-quantity mappings
        self.session.execute(stmt, products)
        self.session.commit()

# %% [markdown]
# Now we can test our methods. Firstly, let's create the products:

# %%
with session_pool() as session:
    repo = Repo(session)
    
    # Create a list of 10 fake products using Python Faker library
    products = [
        dict(
            title=fake.word(),  # Generate a random word as product title
            description=fake.sentence(),  # Generate a random sentence as product description
            price=fake.pyfloat(left_digits=3, right_digits=2, positive=True)  # Generate a random positive float as product price
        ) for _ in range(10)
    ]
    
    # Use the add_bulk_products method to insert these products into the database
    # The method returns the inserted products, possibly with their database IDs
    products = repo.add_bulk_products(products)
    
    # Print the list of inserted products to the console
    for product in products:
        print(product)

# %% [markdown]
# Now we can create an order for the user and add products to it
#
#

# %%
with session_pool() as session:
    repo = Repo(session)

    # Fetch the first user from the database (it's better to create a separate method for this, though)
    user = repo.get_all_users()[0]
    print(user)
    
    # Create a new order for the fetched user and retrieve the generated order_id
    order_id = repo.create_new_order_for_user(user.telegram_id)
    print(f"Order ID: {order_id}")
    
    # Create a list of products with their quantities for the order
    # (Assumption: 'products' is a list of product objects previously fetched or created)
    products = [
        dict(
            product_id=product.product_id,  # Use existing product IDs
            quantity=fake.pyint()  # Generate random quantities using Faker
        ) for product in products
    ]
    
    # Insert the products into the order using the 'bulk_insert_products_into_order_id' method
    repo.bulk_insert_products_into_order_id(order_id, products)

# %% [markdown]
# Let's check if the order was created with the products

# %%
with session_pool() as session:
    repo = Repo(session)
    # Construct a SQL SELECT statement to fetch an order by its order_id
    stmt = select(Order).where(Order.order_id == order_id)  # Replace 'order_id' with a specific value if not already defined

    # Execute the SELECT statement and fetch the result
    result = session.execute(stmt)

    # Retrieve the first scalar result which contains the fetched order information
    order_info = result.scalars().first()

    # Display the fetched order's ID
    print(f"Order ID {order_info.order_id}")

    # Loop through the associated products of the order
    # This is possible because of a presumed relationship between 'Order' and 'OrderProducts' tables
    for order_product in order_info.products:
        print(order_product.product)  # Access and print the product information

# %%
