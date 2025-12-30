import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from mega.src.utils.schema_loader import load_schema

logger = logging.getLogger(__name__)


def loader(spark: SparkSession, src_df: DataFrame, catalog: str, schema: str, table: str) -> bool:
    """
    Loads the data from the source DataFrame into the specified table in the specified schema of the specified catalog.
    """
    try:
        logger.info(f"Loading data into table {catalog}.{schema}.{table}")
        src_df.write.mode("overwrite").saveAsTable(
            f"{catalog}.{schema}.{table}")
        logger.info(
            f"Data loaded successfully into table {catalog}.{schema}.{table}")
        return True
    except Exception as e:
        logger.error(
            f"Error loading data into table {catalog}.{schema}.{table}: {e}")
        return False
