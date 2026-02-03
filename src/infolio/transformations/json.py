import json


def convert_json_to_string(value: any) -> str | None:
    """
    Safely convert a value to a JSON string.

    Handles Python objects, dictionaries, lists, or values with `to_dict()`.
    Returns `None` for null-like values to preserve schema integrity.

    Parameters
    ----------
    value : any
        The object to convert to JSON.

    Returns
    -------
    str | None
        JSON string representation or None if conversion isn't possible.
    """
    if value is None:
        return None
    if hasattr(value, "to_dict"):
        return json.dumps(value.to_dict(as_series=False))
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return json.dumps(str(value))

