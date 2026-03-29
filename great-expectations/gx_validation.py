import great_expectations as gx
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

# Connect via SQLAlchemy
engine = create_engine(URL(
    account='pw18992.eu-central-1',
    user='DBT_USER',
    password='YourStrongPassword123!',
    role='DBT_ROLE',
    warehouse='DBT_WH',
    database='ECOMMERCE',
    schema='RAW'
))

# Load RAW orders into pandas
with engine.connect() as conn:
    df = pd.read_sql(text("SELECT * FROM ECOMMERCE.RAW.ORDERS"), conn)

print(f"✅ Loaded {len(df)} rows from RAW.ORDERS")

# Create GX context
context = gx.get_context()

# Create data source
data_source = context.data_sources.add_pandas(name="orders_source")
data_asset = data_source.add_dataframe_asset(name="raw_orders")
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    "whole_dataframe"
)

# Create expectation suite
suite = gx.ExpectationSuite(name="raw_orders_suite")
suite = context.suites.add(suite)

# Add expectations
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="order_status",
        value_set=["delivered", "shipped", "canceled", "unavailable",
                   "invoiced", "processing", "approved", "created"]
    )
)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=90000,
        max_value=110000
    )
)

# Get batch and validate
batch = batch_definition.get_batch(
    batch_parameters={"dataframe": df}
)
results = batch.validate(suite)

# Print results
print(f"\n✅ Validation Results:")
print(f"Success: {results['success']}")
print(f"Total: {results['statistics']['evaluated_expectations']}")
print(f"Passed: {results['statistics']['successful_expectations']}")
print(f"Failed: {results['statistics']['unsuccessful_expectations']}")