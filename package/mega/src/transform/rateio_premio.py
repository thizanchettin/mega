import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode

logger = logging.getLogger(__name__)

def get_rateio(spark: SparkSession, src_cat: str, src_schema: str, src_tab: str) -> DataFrame:
  """
  Reads the bronze layer table and prepare a DataFrame of Concursos for silver layer
  """
  
  logger.info(f"Processando Rateio do Prêmio")

  df = spark.read.table("mega.bronze.resultados").select("numero", "listaRateioPremio").withColumn("listaRateioPremio", explode("listaRateioPremio")).select("numero", "listaRateioPremio.*").orderBy("numero", "faixa")

  logger.info(f"Rateio do Prêmio processado com sucesso")

  return df