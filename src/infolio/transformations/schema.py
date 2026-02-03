import polars as pl


def enforce_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """
    Ensure a Polars DataFrame conforms to a specified schema.

    This function:
    - Adds missing columns with null values
    - Casts all columns to the types specified in the schema
    - Drops any extra columns not in the schema

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame to sanitize.
    schema : Dict[str, pl.DataType]
        Dictionary mapping column names to desired Polars data types.

    Returns
    -------
    pl.DataFrame
        A DataFrame with all columns matching the specified schema.
    """
    # Add missing columns
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    # Cast columns to correct type and select only schema columns
    df = df.select([pl.col(col).cast(dtype) for col, dtype in schema.items()])

    return df

