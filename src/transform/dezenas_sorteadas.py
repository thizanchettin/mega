import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, posexplode

logger = logging.getLogger(__name__)

def get_dezenas(spark: SparkSession, src_cat: str, src_schema: str, src_tab: str) -> DataFrame:
  """
  Reads the bronze layer table and prepare a DataFrame of Concursos for silver layer
  """

  logger.info(f"Processando Dezenas Sorteadas")

  df = spark.read.table("mega.bronze.resultados").select("numero", posexplode("dezenasSorteadasOrdemSorteio").alias("posicao", "dezena")).withColumn("ordem", col("posicao") + 1).select("numero", "ordem", "dezena").orderBy("numero", "posicao")

  logger.info(f"Dezenas Sorteadas processadas com sucesso")

  return df