import sys
import logging
from typing import Dict, Any, overload, Literal
from datetime import datetime

# Glue modules
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# spark modules
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from producto_transacciones_fraude.extra.constants import (DATA_PRODUCT, MAP_RENAME_COLUMNS, COLUMN_TYPES)
from producto_transacciones_fraude.extra.config.hudiconfig import get_hudi_config
from producto_transacciones_fraude.extra.utils.utils import create_custom_logger
from producto_transacciones_fraude.extra.utils.extract import read_sparkdf_from_catalog
from producto_transacciones_fraude.extra.config.sparkconfig import spark_config
from producto_transacciones_fraude.extra.utils.transform import (
    rename_dataframe, custom_trim_character, apply_column_types,
    add_column_tipo_movimiento, validate_amount, add_tipo_monto_column,
    convert_column_comb_format_unix, add_field_execution_date
)
from producto_transacciones_fraude.extra.utils.load import save_hudi

spark_config: SparkConf = spark_config

def extract(
        glue_context: GlueContext,
        database: str,
        table_name: str,
        additional_options: dict
) -> DataFrame:
    # Leer df
    df: DataFrame = read_sparkdf_from_catalog(
    glue_context=GlueContext,
    database=database,
    table_name=table_name
)
    return df

def transform_product_transacciones_fraude(df: DataFrame) -> DataFrame:

    # 1. Función para renombrar columnas.
    df = rename_dataframe(df=df, map_columns=MAP_RENAME_COLUMNS)

    # 2. Trim apply for columns of DataFrame
    df = custom_trim_character(df, "type", "name_orig","name_dest")

    # 3. Función para tipar el tipo de dato de cada columna
    df = apply_column_types(df, COLUMN_TYPES)

    # 4. Función para agregar columna tipo_movimiento.
    df = add_column_tipo_movimiento(df)

    # 5. Función para validar trx (amount) con valores negativos.
    df = validate_amount(df)

    # 6. Funcion para agregar validación tipo de monto transado.
    df = add_tipo_monto_column(df)

    # 7. Obtener la fecha actual como cadena en el formato '%Y-%m-%dT%H:%M:%S'
    execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 7. Add field execution_date
    df = add_field_execution_date(df, execution_date)

    # 8. Convirtiendo columna execution_date en formato Unix, para crear la llave precombinada
    df = convert_column_comb_format_unix(df)

    return df

def main():
    # Input Job Parameters
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME"
        ]
    )

    # Iniciar Spark
    APP_NAME: str = DATA_PRODUCT

    spark: SparkSession = (
        SparkSession.builder.appName(APP_NAME).enableHiveSupport().config(conf=spark_config).getOrCreate()
    )
    glue_context = GlueContext(spark.sparkContext)
    logger = create_custom_logger(__name__)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Starting Glue job...")

    """ Variables del productos de datos """

    mode = "append"

    # Database raw desde el catalogo
    database="s3_db_trx_fraude"
    table_name = "raw_prueba_nequi_input"

    # Databases
    database_gestionfraude_curated = "s3_db_trx_fraude"
    dp_table_name = "processed_tbl_trx_fraude"
    
    # Buckets
    dp_post_stage_path = "s3://prueba-nequi-processed/curated/"


    """ Variables y conf HUDI para el producto de datos"""
    dp_path = "s3://prueba-nequi-processed/curated/"
    dp_partition_field = "step"
    dp_primary_key = "primary_key"

    # Parametros configuración Hudi
    #logs_hudi_config = get_hudi_config(
    #    database=database_gestionfraude_curated,
    #    table_name=dp_table_name,
    #    primary_key=dp_primary_key,
   #   partition_field=dp_partition_field
    #)

    save_hudi_config = get_hudi_config(
        database="s3_db_trx_fraude",
        table_name="processed_tbl_trx_fraude",
        primary_key="primary_key",
        partition_field="step"
    )

    try:
        """ PRODUCTO DE DATOS """
        # Extract raw data
        logger.info(f"Leyendo data de s3: {dp_path}")
        # Read dataframes (sources)
        df: DataFrame = read_sparkdf_from_catalog(
            glue_context=glue_context,
            database=database,
            table_name=table_name
        )

        logger.info(f"Lectura completada de s3: {dp_path}, count df: {df.count()}")

        if df.count() > 0:

            """ PRODUCTO DE DATOS : transacciones_fraude_etl """
            # Transform data
            logger.info(f"Iniciando Transformacion en: {dp_table_name}")
            df_dp_final = transform_product_transacciones_fraude(df)
            logger.info(f"Finalizando Transformacion en: {dp_table_name}, count: {df_dp_final.count()}")
            # Loading data
            logger.info(f"Cargando datos con Hudi config: {save_hudi_config}", )
            logger.info(f"Cargando datos DP ... en: {dp_post_stage_path}")
            save_hudi(spark_df=df_dp_final,
                      hudi_options=save_hudi_config,
                      path=dp_post_stage_path,
                      write_format="hudi",
                      mode=mode
                      )
            job.commit()
            logger.info("Data Product guardado exitosamente!!")
        else:
            logger.info(f"Sin info nueva en el prefix s3: {dp_path}")
    except Exception as error:
        logger.error(f"Falla el proceso en main {error}")
        raise error

if __name__ == '__main__':
    main()