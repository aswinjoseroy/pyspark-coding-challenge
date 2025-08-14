from datetime import datetime, timedelta

from pyspark.sql import Row

from src.transformations import aggregate_actions
from src.transformations import explode_impressions
from src.transformations import normalize_action_schema
from src.transformations import pad_array


def test_normalize_action_schema(spark):
    clicks_df = spark.createDataFrame([
        Row(customer_id=1, item_id=101, click_time=datetime(2024, 1, 1, 10, 0, 0)),
        Row(customer_id=2, item_id=102, click_time=datetime(2024, 1, 2, 11, 0, 0)),
    ])

    add_to_carts_df = spark.createDataFrame([
        Row(customer_id=1, config_id=201, simple_id=1, occurred_at=datetime(2024, 1, 1, 12, 0, 0)),
    ])

    orders_df = spark.createDataFrame([
        Row(customer_id=2, config_id=301, simple_id=2, occurred_at=datetime(2023, 12, 31, 9, 0, 0)),
    ])

    result_df = normalize_action_schema(clicks_df, add_to_carts_df, orders_df)
    result = result_df.collect()

    # Verify schema correctness
    for row in result:
        assert set(row.asDict().keys()) == {"customer_id", "item_id", "action_type", "action_time"}

    # Verify action_type mapping
    clicks_types = [r.action_type for r in result if r.item_id in [101, 102]]
    carts_types = [r.action_type for r in result if r.item_id == 201]
    orders_types = [r.action_type for r in result if r.item_id == 301]

    assert all(t == 1 for t in clicks_types)
    assert all(t == 2 for t in carts_types)
    assert all(t == 3 for t in orders_types)


def test_explode_impressions(spark):
    data = [
        Row(customer_id=1, dt="2025-08-10", impressions=[
            Row(item_id=101, is_order=False),
            Row(item_id=102, is_order=True)
        ])
    ]
    df = spark.createDataFrame(data)

    result_df = explode_impressions(df)
    result = [(row.customer_id, row.dt, row.item_id, row.is_order) for row in result_df.collect()]

    expected = [
        (1, "2025-08-10", 101, False),
        (1, "2025-08-10", 102, True)
    ]

    assert result == expected


def test_aggregate_actions(spark):
    # Create sample input DataFrame
    now = datetime(2025, 8, 13, 12, 0, 0)
    data = [
        (1, now - timedelta(minutes=5), 101, 1),  # click
        (1, now - timedelta(minutes=3), 102, 2),  # add_to_cart
        (1, now - timedelta(minutes=1), 103, 3),  # order
        (1, now - timedelta(minutes=10), 104, 1), # oldest, should be dropped if n=3
        (2, now - timedelta(minutes=2), 201, 1),  # click
        (2, now - timedelta(minutes=1), 202, 3),  # order
    ]
    columns = ["customer_id", "action_time", "item_id", "action_type"]
    actions_df = spark.createDataFrame(data, schema=columns)

    # Run transformation
    result_df = aggregate_actions(actions_df, n=3)

    # Collect results
    result = {row["customer_id"]: (row["actions"], row["action_types"]) for row in result_df.collect()}

    # Assertions
    assert set(result.keys()) == {1, 2}

    # Customer 1 → last 3 actions in descending order by action_time
    assert result[1][0] == [103, 102, 101]  # item_ids
    assert result[1][1] == [3, 2, 1]        # action_types

    # Customer 2 → only 2 actions available
    assert result[2][0] == [202, 201]       # item_ids
    assert result[2][1] == [3, 1]           # action_types


def test_pad_array(spark):
    # Sample input: two customers with different numbers of actions and action_types
    data = [
        (1, [101, 102, 103], [1, 2, 3]),
        (2, [201], [1])
    ]
    columns = ["customer_id", "actions", "action_types"]
    actions_df = spark.createDataFrame(data, columns)

    # Apply padding
    length = 5
    pad_value = 0
    padded_df = pad_array(actions_df, length=length, pad_value=pad_value)

    # Collect results
    result = {row["customer_id"]: (row["actions"], row["action_types"]) for row in padded_df.collect()}

    # Assertions
    assert len(result[1][0]) == length
    assert len(result[1][1]) == length
    assert len(result[2][0]) == length
    assert len(result[2][1]) == length

    # Check padding values at the end
    assert result[1][0] == [101, 102, 103, 0, 0]
    assert result[1][1] == [1, 2, 3, 0, 0]
    assert result[2][0] == [201, 0, 0, 0, 0]
    assert result[2][1] == [1, 0, 0, 0, 0]
