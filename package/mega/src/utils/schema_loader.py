import json
from pyspark.sql.types import StructType


def load_schema(schema_path: str) -> StructType:
    """
    Carrega um StructType a partir de um arquivo JSON.
    """
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    return StructType.fromJson(schema_json)
