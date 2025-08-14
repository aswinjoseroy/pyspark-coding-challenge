from pyspark.sql import SparkSession

from src.transformations import explode_impressions, aggregate_actions, pad_array, normalize_action_schema


def run_pipeline(impressions_df, clicks_df, add_to_carts_df, orders_df, max_actions=10):
    """
    End-to-end pipeline chaining all transformations to produce model-ready data.
    """
    # Step 1: Explode impressions into one row per item
    exploded_impressions_df = explode_impressions(impressions_df)
    exploded_impressions_df.show()

    # Step 2: Aggregate all actions into a unified DataFrame
    aggregated_actions_df = aggregate_actions(normalize_action_schema(clicks_df, add_to_carts_df, orders_df))
    aggregated_actions_df.show()

    # Step 3: Pad arrays for model input
    padded_df = pad_array(aggregated_actions_df, length=max_actions)
    padded_df.show()

    # Step 4: Join exploded impressions with aggregated actions
    final_df = exploded_impressions_df.join(
        padded_df,
        on=['customer_id'],
        how='left'
    )

    return final_df


if __name__ == "__main__":
    pass