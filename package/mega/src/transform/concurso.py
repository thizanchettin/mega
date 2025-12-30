import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

def get_concursos(spark: SparkSession, src_cat: str, src_schema: str, src_tab: str) -> DataFrame:
        """
        Reads the bronze layer table and prepare a DataFrame of Concursos for silver layer
        """

        logger.info(f"Processando concursos")
        
        df = spark.read.table(f"{src_cat}.{src_schema}.{src_tab}")\
                        .select("numero", "dataApuracao", "dataProximoConcurso", \
                                "indicadorConcursoEspecial", "acumulado", "localSorteio", \
                                "nomeMunicipioUFSorteio", "numeroConcursoAnterior", "numeroConcursoFinal_0_5", \
                                "numeroConcursoProximo", "valorArrecadado", "valorAcumuladoConcurso_0_5", \
                                "valorAcumuladoConcursoEspecial", "valorAcumuladoProximoConcurso", \
                                "valorEstimadoProximoConcurso") \
                        .orderBy("numero")
        
        logger.info(f"Concursos processados com sucesso")

        return df