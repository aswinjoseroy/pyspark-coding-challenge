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
    from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, BooleanType
    from pyspark.sql.functions import from_json, col

    spark = SparkSession.builder.master(("local[*]")).appName("transformer_feeder").getOrCreate()

    # Example input DataFrames (replace with real read logic)
    impressions_df = spark.read.csv("/Users/aswinjoseroy/PycharmProjects/pyspark-coding-challenge/data/impressions.csv",
                                    header=True, inferSchema=True, multiLine=True, escape='"')
    item_schema = ArrayType(
        StructType([
            StructField("item_id", IntegerType(), True),
            StructField("is_order", BooleanType(), True)
        ])
    )
    impressions_df = impressions_df.withColumn("impressions", from_json(col("impressions"), item_schema))

    clicks_df = spark.read.csv("/Users/aswinjoseroy/PycharmProjects/pyspark-coding-challenge/data/clicks.csv", header=True, inferSchema=True)
    add_to_carts_df = spark.read.csv("/Users/aswinjoseroy/PycharmProjects/pyspark-coding-challenge/data/add_to_carts.csv", header=True, inferSchema=True)
    orders_df = spark.read.csv("/Users/aswinjoseroy/PycharmProjects/pyspark-coding-challenge/data/pre_orders.csv", header=True, inferSchema=True)

    impressions_df.printSchema()
    clicks_df.printSchema()
    add_to_carts_df.printSchema()
    orders_df.printSchema()

    print("pipeline starts")

    final_output_df = run_pipeline(impressions_df, clicks_df, add_to_carts_df, orders_df)

    print(final_output_df.show())

    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType

    # Cast all columns to string
    final_output_df_str = final_output_df.select([col(c).cast(StringType()) for c in final_output_df.columns])

    # Write to CSV
    final_output_df_str.coalesce(1).write.csv(
        "/Users/aswinjoseroy/PycharmProjects/pyspark-coding-challenge/data/final.csv", header=True,
        mode="overwrite")
