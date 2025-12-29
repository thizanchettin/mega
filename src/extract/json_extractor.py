import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from src.utils.schema_loader import load_schema

logger = logging.getLogger(__name__)


def extract_json(
    spark: SparkSession,
    source_path: str,
    schema_path: str,
    multiline: bool = False,
    corrupt_column: str = "_corrupt_record"
) -> DataFrame:
    """
    Extract genérico de JSON com schema externo.

    - Funciona para qualquer JSON
    - Schema vem de arquivo
    - Captura registros corruptos
    - Nenhuma lógica de negócio
    """

    logger.info(f"Lendo JSON de {source_path}")
    logger.info(f"Usando schema {schema_path}")

    schema = load_schema(schema_path)

    df = (
        spark.read
             .option("multiline", multiline)
             .option("columnNameOfCorruptRecord", corrupt_column)
             .schema(schema)
             .json(source_path)
    )

    total = df.count()

    if corrupt_column in df.columns:
        corrupt = df.filter(col(corrupt_column).isNotNull()).count()
    else:
        corrupt = 0

    logger.info(f"Total registros: {total}")
    logger.info(f"Registros corruptos: {corrupt}")

    if corrupt > 0:
        logger.warning("Existem registros corruptos na ingestão")

    return df
