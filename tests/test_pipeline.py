from pyspark.sql import Row
from src.pipeline import explode_impressions, filter_actions_before_date


def test_explode_impressions(spark):
    # Sample input
    impressions = [
        Row(dt="2025-08-01", ranking_id="r1", customer_id=1,
            impressions=[Row(item_id=101, is_order=True), Row(item_id=102, is_order=False)])
    ]
    df = spark.createDataFrame(impressions)

    exploded_df = explode_impressions(df)
    result = exploded_df.collect()

    assert len(result) == 2
    assert result[0]["item_id"] == 101
    assert result[0]["is_order"] is True
    assert result[1]["item_id"] == 102
    assert result[1]["is_order"] is False


def test_filter_actions_before_date(spark):
    actions = [
        Row(customer_id=1, item_id=101, occurred_at="2025-07-31"),
        Row(customer_id=1, item_id=102, occurred_at="2025-08-02")
    ]
    impressions = [
        Row(dt="2025-08-01", customer_id=1)
    ]

    actions_df = spark.createDataFrame(actions)
    impressions_df = spark.createDataFrame(impressions)

    filtered_df = filter_actions_before_date(actions_df, impressions_df)
    result = filtered_df.collect()

    assert len(result) == 1
    assert result[0]["item_id"] == 101
