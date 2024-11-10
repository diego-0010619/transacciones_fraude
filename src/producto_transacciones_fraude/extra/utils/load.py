import sys
from pyspark.sql import DataFrame
from typing import Literal
from producto_transacciones_fraude.extra.utils.utils import create_custom_logger

logger = create_custom_logger(__name__)


def save_hudi(
        spark_df: DataFrame = None,
        hudi_options: dict = None,
        path: str = None,
        write_format: str = "hudi",
        mode: str = "upsert"
):
    """Funcion para guardar datos en formato hudi en el catálogo de glue
    """

    try:
        print("iniciando")
        spark_df.write.format(write_format).mode(mode).options(**hudi_options).save(path)
        #logger.info(f"Guardado exitoso de la tabla de hudi en db:, tabla:")
    except Exception as write_error:
        #logger.error(f"Error @ {sys._getframe().f_code.co_name}: {write_error}")
        print(f"Error @ {sys._getframe().f_code.co_name}: {write_error}")
        #raise RuntimeError from write_error
