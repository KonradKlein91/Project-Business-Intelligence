from decimal import Decimal
import time as time_module
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from datetime import datetime, timedelta
import random
import os


start_time = time_module.time()

# Remove the existing database file if it exists
if os.path.exists('star_schema.db'):
    os.remove('star_schema.db')

# Create a SQLite database
engine = create_engine('sqlite:///star_schema.db', echo=False)
Base = declarative_base()


# Define the tables
class DimProduct(Base):
    __tablename__ = 'dim_product'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)
    unit_price = Column(Numeric(10, 2))


class DimTime(Base):
    __tablename__ = 'dim_time'
    time_id = Column(Integer, primary_key=True)
    transaction_datetime = Column(DateTime)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    minute = Column(Integer)


class DimLocation(Base):
    __tablename__ = 'dim_location'
    location_id = Column(Integer, primary_key=True)
    store_name = Column(String)
    address = Column(String)
    phone_number = Column(String)


class FactReceipt(Base):
    __tablename__ = 'fact_receipt'
    receipt_id = Column(Integer, primary_key=True)
    time_id = Column(Integer, ForeignKey('dim_time.time_id'), nullable=False)
    location_id = Column(Integer, ForeignKey('dim_location.location_id'), nullable=False)
    receipt_number = Column(String)
    transaction_number = Column(String)
    total_amount = Column(Numeric(10, 2))
    cash_given = Column(Numeric(10, 2))
    change_given = Column(Numeric(10, 2))
    tax_amount = Column(Numeric(10, 2))
    cashier_name = Column(String)

    time = relationship("DimTime")
    location = relationship("DimLocation")


class FactReceiptPosition(Base):
    __tablename__ = 'fact_receipt_position'
    position_id = Column(Integer, primary_key=True)
    receipt_id = Column(Integer, ForeignKey('fact_receipt.receipt_id'), nullable=False)
    product_id = Column(Integer, ForeignKey('dim_product.product_id'), nullable=False)
    quantity = Column(Numeric(10, 2))
    total_price = Column(Numeric(10, 2))

    receipt = relationship("FactReceipt")
    product = relationship("DimProduct")


# Create the tables in the database
Base.metadata.create_all(engine)

# Create a session to interact with the database
session = Session(engine)

data_gen_start = time_module.time()

# Insert sample data
# DimProduct
products = [
    DimProduct(product_name=name, unit_price=round(random.uniform(1, 20), 2))
    for name in ["Apple", "Banana", "Orange", "Milk", "Bread", "Cheese", "Eggs", "Cereal", "Coffee", "Tea",
                 "Chicken", "Beef", "Fish", "Rice", "Pasta", "Tomato", "Potato", "Onion", "Carrot", "Lettuce"]
]
session.add_all(products)

# DimTime
base_time = datetime(2023, 1, 1)
times = [
    DimTime(transaction_datetime=(base_time + timedelta(hours=i)).replace(microsecond=0),
            year=(base_time + timedelta(hours=i)).year,
            month=(base_time + timedelta(hours=i)).month,
            day=(base_time + timedelta(hours=i)).day,
            hour=(base_time + timedelta(hours=i)).hour,
            minute=0)
    for i in range(24 * 30)  # 30 days of hourly entries
]
session.add_all(times)

# DimLocation
locations = [
    DimLocation(store_name=f"Store {i}", address=f"{i}00 Main St", phone_number=f"555-{1000 + i}")
    for i in range(1, 6)  # 5 stores
]
session.add_all(locations)

# Commit to get IDs
session.commit()

# FactReceipt
receipts = []
for i in range(1000):
    random_time = random.choice(times)
    location = random.choice(locations)

    receipt = FactReceipt(
        time_id=random_time.time_id,
        location_id=location.location_id,
        cashier_name=f"Cashier {random.randint(1, 10)}"
    )
    receipts.append(receipt)

session.add_all(receipts)

# Commit to get receipt IDs
session.commit()

# Now set the receipt_number and transaction_number based on the auto-generated receipt_id
for receipt in receipts:
    receipt.receipt_number = f"R{receipt.receipt_id:04d}"
    receipt.transaction_number = f"T{receipt.receipt_id:04d}"

session.commit()

data_gen_end = time_module.time()
data_gen_time = data_gen_end - data_gen_start

insert_start = time_module.time()

# FactReceiptPosition
positions = []
for receipt in receipts:
    num_items = random.randint(1, 5)  # 1 to 5 items per receipt
    receipt_total = Decimal('0.00')
    for _ in range(num_items):
        product = random.choice(products)
        quantity = Decimal(str(random.randint(1, 3)))
        total_price = quantity * Decimal(str(product.unit_price))
        receipt_total += total_price

        positions.append(
            FactReceiptPosition(
                receipt_id=receipt.receipt_id,
                product_id=product.product_id,
                quantity=quantity,
                total_price=total_price.quantize(Decimal('0.01'))
            )
        )

    # Update the receipt's total amount
    receipt.total_amount = receipt_total.quantize(Decimal('0.01'))
    receipt.tax_amount = (receipt_total * Decimal('0.1')).quantize(Decimal('0.01'))
    receipt.cash_given = (receipt_total + receipt.tax_amount + Decimal(str(random.uniform(0, 5)))).quantize(
        Decimal('0.01'))
    receipt.change_given = (receipt.cash_given - (receipt_total + receipt.tax_amount)).quantize(Decimal('0.01'))

session.add_all(positions)

# Final commit
session.commit()

insert_end = time_module.time()
insert_time = insert_end - insert_start

end_time = time_module.time()
total_time = end_time - start_time

print(f"Star schema created and 1000 sample receipts inserted successfully in 'star_schema.db'")
print(f"Total execution time: {total_time:.2f} seconds")
print(f"Data generation time: {data_gen_time:.2f} seconds")
print(f"Data insertion time: {insert_time:.2f} seconds")