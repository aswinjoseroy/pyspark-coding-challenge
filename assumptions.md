Assumptions 

    1. Schema Consistency

        config_id in add-to-carts and orders is the same as item_id in impressions/clicks.

        Timestamps are in the same timezone across datasets.

    2. Historical Window

        "At least one year" means we keep all historical actions up to 365 days back from the impression date, but we do not cap at 1 year if more exists — we just need the most recent 1000 actions, and they might all be within the last few days.

    3. Action Deduplication

        Duplicates in historical actions are kept (spec explicitly says duplicates are useful).

    4. Order of Actions

        Sort by action_time (or occurred_at for add-to-carts/orders, click_time for clicks), descending, tie-broken arbitrarily.

    5. Padding

        If fewer than 1000 historical actions exist, pad with:

            0 for item IDs in actions

            0 for action types in action_types

    6. Impression Explosion

        The impressions array is exploded into one row per (customer_id, item_id, dt).

    7. Data Volume

        We should assume input scale is huge (billions of rows over a year), so pipeline must be partition-aware and memory-efficient.

    8. Training Input Partitioning

        Output can be partitioned by dt for efficient daily training.


Ambiguities / Unclear Points in the Assignment

  1. "At least one year" retention

Resolution:

    Keep all historical actions before the impression date, even if older than one year.

    Only filter by the most recent 1000 actions, so older actions will naturally be excluded if the sequence is longer than 1000.

2. Exact mapping of embedding IDs

Resolution:

    Pass raw item_id (or config_id) in the output.

    Let data scientists map to embeddings later, as the spec explicitly says that will be handled in the model training step.

3. Handling same-day actions

Resolution:

    Exclude all actions from the same dt as the impression to avoid data leakage.

    No impression timestamps are provided, so it’s safest to ignore same-day actions entirely.

4. Tie-breaking in ordering

Resolution:

    Sort actions by action_time DESC (or occurred_at DESC) and break ties using item_id ASC.

    This is deterministic and simple to implement in Spark.

5. Multiple item variants (simple_id)

Resolution:

    Ignore simple_id for the training input.

    Use only item_id / config_id as the unique identifier for embedding lookup.

6. Missing impressions

Resolution:

    Skip customers without impressions for that day.

    Since the model only trains on impressions, customers who did not log in do not contribute data.

7. Action time granularity

Resolution:

    Use the more granular timestamp when available:

        click_time for clicks

        occurred_at for add-to-cart and previous orders

    Fallback to order_date only if occurred_at is missing.

8. Performance expectations

Resolution:

    Use partitioned reads by dt (date) wherever possible.

    Precompute historical actions per customer with window functions.

    Cache intermediate DataFrames for repeated operations.

    Avoid collecting data to the driver; all joins and aggregations stay in Spark.
