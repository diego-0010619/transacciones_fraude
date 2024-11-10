import sys
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from producto_transacciones_fraude.extra.utils.utils import create_custom_logger

logger = create_custom_logger(__name__)

def read_sparkdf_from_catalog(
        glue_context: GlueContext,
        database: str = "",
        table_name: str = ""
) -> DataFrame:
    """
    Return a dataframe from read glue catalog table.

    Args:
        glue_context (GlueContext): Glue Job Context
        database (str): Name database Glue Data Catalog
        table_name (str): Name table Glue Data Catalog

    Returns:
        Dataframe: Spark's Dataframe
    """
    try:
        logger.info(f"Leyendo dataframe desde el catálogo: {database}, tabla: {table_name}")

        df: "DataFrame" = glue_context.create_data_frame.from_catalog(
            database=database,
            table_name=table_name
        )
        return df

    except Exception as read_error:
        logger.exception(f"Error @ {sys._getframe().f_code.co_name}: {read_error}")
        raise