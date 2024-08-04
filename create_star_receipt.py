from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.ext.automap import automap_base

# Use the newer method to create the base
Base = declarative_base()

# Create engine and reflect the database
engine = create_engine('sqlite:///star_schema.db', echo=False)
Base.metadata.reflect(engine)

# Use automap to generate the ORM classes
AutomapBase = automap_base()
AutomapBase.prepare(engine, reflect=True)

# Get the reflected table classes
FactReceipt = AutomapBase.classes.fact_receipt
FactReceiptPosition = AutomapBase.classes.fact_receipt_position
DimProduct = AutomapBase.classes.dim_product
DimTime = AutomapBase.classes.dim_time
DimLocation = AutomapBase.classes.dim_location

def print_receipt(receipt_id):
    with Session(engine) as session:
        # Query to get receipt details
        receipt_query = select(FactReceipt, DimTime, DimLocation).join(
            DimTime, FactReceipt.time_id == DimTime.time_id
        ).join(
            DimLocation, FactReceipt.location_id == DimLocation.location_id
        ).where(FactReceipt.receipt_id == receipt_id)

        receipt_result = session.execute(receipt_query).first()

        if not receipt_result:
            print(f"Receipt with ID {receipt_id} not found.")
            return

        # Unpack the result correctly
        receipt, time, location = receipt_result

        # Query to get receipt items
        items_query = select(FactReceiptPosition, DimProduct).join(
            DimProduct, FactReceiptPosition.product_id == DimProduct.product_id
        ).where(FactReceiptPosition.receipt_id == receipt_id)

        items_result = session.execute(items_query).fetchall()

        # Print receipt
        print("\n" + "=" * 40)
        print(f"Receipt Number: {receipt.receipt_number}")
        print(f"Transaction Number: {receipt.transaction_number}")
        print(f"Date: {time.transaction_datetime}")
        print(f"Store: {location.store_name}")
        print(f"Address: {location.address}")
        print(f"Phone: {location.phone_number}")
        print(f"Cashier: {receipt.cashier_name}")
        print("-" * 40)
        print("Items:")
        for item, product in items_result:
            print(f"  {product.product_name:<15} {item.quantity:>5} x ${item.unit_price:.2f} = ${item.total_price:.2f}")
        print("-" * 40)
        print(f"Subtotal: ${receipt.total_amount - receipt.tax_amount:.2f}")
        print(f"Tax: ${receipt.tax_amount:.2f}")
        print(f"Total: ${receipt.total_amount:.2f}")
        print(f"Cash Given: ${receipt.cash_given:.2f}")
        print(f"Change: ${receipt.change_given:.2f}")
        print("=" * 40)

# Example usage
print_receipt(10)  # Print receipt with ID 1