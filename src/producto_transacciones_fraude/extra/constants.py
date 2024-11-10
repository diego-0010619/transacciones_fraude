from pyspark.sql.types import StringType, IntegerType, DecimalType

# Constantes por cada productos de datos
DATA_PRODUCT: str = "transacciones_fraude"

# Definiendo mapeo para cambiar nombre de las columnas
MAP_RENAME_COLUMNS = {
    "step"              : "step",
    "type"              : "type",
    "amount"            : "amount",
    "nameorig"          : "name_orig",
    "oldbalanceorg"     : "old_balance_org",
    "newbalanceorig"    : "new_balance_orig",
    "namedest"          : "name_dest",
    "oldbalancedest"    : "old_balance_dest",
    "newbalancedest"    : "new_balance_dest",
    "isfraud"           : "is_fraud",
    "isflaggedfraud"    : "is_flagged_fraud"
}

# Definiendo tipos de datos de las variables.
COLUMN_TYPES = [
    ("step", IntegerType()),
    ("type", StringType()),
    ("amount", DecimalType(18, 4)),
    ("name_orig", StringType()),
    ("old_balance_org", DecimalType(18, 4)),
    ("new_balance_orig", DecimalType(18, 4)),
    ("name_dest", StringType()),
    ("old_balance_dest", DecimalType(18, 4)),
    ("new_balance_dest", DecimalType(18, 4)),
    ("is_fraud", IntegerType()),
    ("is_flagged_fraud", IntegerType())
]
