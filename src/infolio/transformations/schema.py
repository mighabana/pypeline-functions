import polars as pl


def enforce_schema(df: pl.DataFrame, schema: pl.Schema | dict[str, pl.DataType]) -> pl.DataFrame:
    """
    Coerce a Polars DataFrame to conform to a specified schema.

    This function:
    - Adds missing columns with null values
    - Casts all columns to the types specified in the schema
    - Drops any extra columns not in the schema

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame to sanitize.
    schema : pl.Schema | Dict[str, pl.DataType]
        Mapping of column names to desired Polars data types. Accepts either a native ``pl.Schema or a plain dict.

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

