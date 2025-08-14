from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import col, explode


def normalize_action_schema(clicks_df, add_to_carts_df, orders_df):
    """
    Normalizes and unifies actions data into a single schema:
    customer_id, item_id, action_type, action_time
    """
    # Map clicks
    clicks_norm = clicks_df.select(
        F.col("customer_id"),
        F.col("item_id"),
        F.lit(1).alias("action_type"),
        F.col("click_time").alias("action_time")
    )

    # Map add-to-carts
    carts_norm = add_to_carts_df.select(
        F.col("customer_id"),
        F.col("config_id").alias("item_id"),
        F.lit(2).alias("action_type"),
        F.col("occurred_at").alias("action_time")
    )

    # Map orders
    orders_norm = orders_df.select(
        F.col("customer_id"),
        F.col("config_id").alias("item_id"),
        F.lit(3).alias("action_type"),
        F.col("occurred_at").alias("action_time")
    )

    # Union all
    unified_df = clicks_norm.unionByName(carts_norm).unionByName(orders_norm)

    return unified_df


def aggregate_actions(actions_df: DataFrame, n: int = 1000) -> DataFrame:
    """
    Keep only the last `n` actions per customer, ordered by timestamp descending,
    and return them as an array column (ordered by most recent first).

    Parameters:
        actions_df: DataFrame with columns
            ['customer_id', 'action_time', 'item_id', 'action_type']
        n: number of last actions to keep per customer

    Returns:
        DataFrame with columns:
            ['customer_id', 'actions'] where actions is an array of item_ids
    """
    # Window for ordering actions by recency within each customer
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("action_time").desc())

    # Keep only the last n actions
    filtered_df = (
        actions_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") <= n)
    )

    # Collect actions into an array, preserving order
    actions_array_df = (
        filtered_df
        .groupBy("customer_id")
        .agg(
            F.collect_list("item_id").alias("actions"),
            F.collect_list("action_type").alias("action_types")
        )
    )

    return actions_array_df


def explode_impressions(impressions_df: DataFrame) -> DataFrame:
    """
    Explodes the 'impressions' array per customer into individual rows.

    Parameters:
        impressions_df: DataFrame with columns ['customer_id', 'dt', 'impressions']
                        where 'impressions' is an array of structs: {'item_id': int, 'is_order': bool}

    Returns:
        DataFrame with columns ['customer_id', 'dt', 'item_id', 'is_order']
    """
    # Explode the array column
    exploded_df = impressions_df.withColumn("impression", explode("impressions"))

    # Extract struct fields into separate columns
    exploded_df = exploded_df.select(
        col("customer_id"),
        col("dt"),
        col("impression.item_id").alias("item_id"),
        col("impression.is_order").alias("is_order")
    )

    return exploded_df


def pad_array(actions_df: DataFrame, length: int = 1000, pad_value=0) -> DataFrame:
    """
    Pads the 'actions' array column in the DataFrame to a fixed length.

    Parameters:
        actions_df: DataFrame with columns ['customer_id', 'actions']
        length: desired length of the array
        pad_value: value used for padding

    Returns:
        DataFrame with ['customer_id', 'actions'] where each array has exactly `length` elements
    """
    padded_df = actions_df.withColumn(
        "actions",
        F.expr(
            f"slice(concat(actions, array_repeat({pad_value}, {length})), 1, {length})"
        )
    )
    return padded_df
