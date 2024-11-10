from typing import overload, Literal
from producto_transacciones_fraude.extra.utils.utils import create_custom_logger

logger = create_custom_logger("config")


def get_hudi_config(
        database: str,
        table_name: str,
        primary_key: str,
        operation: Literal['upsert', 'insert', 'bulk_insert', 'insert_overwrite'] = 'upsert',
        partition_field: str = 'step'
) -> dict:
    """
    Generates a configuration dictionary for Hudi based on the provided parameters.

    Args:
        database (str): The name of the Hive database.
        table_name (str): The name of the Hive table.
        primary_key (str): The primary key field for the Hudi table.
        operation (Literal['upsert', 'insert', 'bulk_insert', 'insert_overwrite'], optional): The write operation type. Defaults to 'upsert'.
        partition_field (str, optional): The field used for partitioning the table. Defaults to 'step'.

    Returns:
        dict: A dictionary containing the Hudi configuration.
    """
    # Use the primary_key directly as there is no precombine_field provided
    _hudi_config: dict[str, str | Literal] = {
        'hoodie.table.name': table_name,
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.recordkey.field": primary_key,
        "hoodie.datasource.write.precombine.field": primary_key,  # Use primary_key instead of precombine_field
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.support_timestamp": "true"
    }

    # Add partition configuration if 'step' is used as the partition field
    if partition_field:
        partition_config = {
            "hoodie.datasource.write.partitionpath.field": partition_field,
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.datasource.hive_sync.partition_fields": partition_field,
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor"}
        _hudi_config.update(partition_config)
        
    return _hudi_config

# # Databases
# database_productos_curated = "co_delfos_gestionfraude_datalab_pdn"

# save_hudi_config = get_hudi_config(database=database_productos_curated,
#                                               table_name=table_name,
#                                               primary_key="primary_key"
#                                               )

# print(save_hudi_config)
