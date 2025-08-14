from pyspark.sql import DataFrame



def normalize_action_schema(clicks_df, add_to_carts_df, orders_df):
    """
    Normalizes and unifies actions data into a single schema:
    customer_id, item_id, action_type, action_time
    """
    pass


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
    pass


def explode_impressions(impressions_df: DataFrame) -> DataFrame:
    """
    Explodes the 'impressions' array per customer into individual rows.

    Parameters:
        impressions_df: DataFrame with columns ['customer_id', 'dt', 'impressions']
                        where 'impressions' is an array of structs: {'item_id': int, 'is_order': bool}

    Returns:
        DataFrame with columns ['customer_id', 'dt', 'item_id', 'is_order']
    """
    pass

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
    pass