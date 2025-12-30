import argparse
import logging
from pyspark.sql import SparkSession
from mega.src.extract.json_extractor import extract_json
from mega.src.transform.dezenas_sorteadas import get_dezenas
from mega.src.transform.concurso import get_concursos
from mega.src.transform.rateio_premio import get_rateio
from mega.src.load.loader import loader


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--schema-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Job iniciado - Ambiente: {args.env}")

    df = extract_json(spark, args.source_path, args.schema_path, True)

    loader(spark, df, "mega", "bronze", "resultados")

    df = get_concursos(spark, "mega", "bronze", "resultados")

    loader(spark, df, "mega", "silver", "concursos")

    df = get_dezenas(spark, "mega", "bronze", "resultados")

    loader(spark, df, "mega", "silver", "dezenas")

    df = get_rateio(spark, "mega", "bronze", "resultados")

    loader(spark, df, "mega", "silver", "lista_rateio")

    logger.info("Job finalizado com sucesso")


if __name__ == "__main__":
    main()
