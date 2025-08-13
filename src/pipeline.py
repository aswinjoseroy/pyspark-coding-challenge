from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col


def explode_impressions(df: DataFrame) -> DataFrame:
    """
    Explode the impressions list into individual rows
    Each row becomes one impression with its item_id and is_order flag.
    """
    return df.select(
        col("dt"),
        col("ranking_id"),
        col("customer_id"),
        explode(col("impressions")).alias("item")
    ).select(
        col("dt"),
        col("ranking_id"),
        col("customer_id"),
        col("item.item_id"),
        col("item.is_order")
    )


def filter_actions_before_date(actions_df: DataFrame, impression_df: DataFrame) -> DataFrame:
    """
    Keep only actions that occurred before the impression date
    """
    return actions_df.join(impression_df.select("dt", "customer_id").distinct(), on="customer_id") \
                     .filter(col("occurred_at") < col("dt"))
