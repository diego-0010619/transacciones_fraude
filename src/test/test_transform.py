import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from src.producto_transacciones_fraude.extra.utils.transform import (rename_dataframe, custom_trim_character, apply_column_types,
        add_column_tipo_movimiento, validate_amount, add_tipo_monto_column,
        convert_column_comb_format_unix, add_field_execution_date
)
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

# INICIO Prueba unitaria de la función rename_dataframe
@pytest.fixture
def spark():
    spark: SparkSession = SparkSession.builder.appName("test_glue_job").getOrCreate()
    return spark

@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", FloatType(), True),
        StructField("newbalanceOrig", FloatType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", FloatType(), True),
        StructField("newbalanceDest", FloatType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
    ])
    
    data = [
        (1, "TRANSFER", 9839.64, "C12345", 170136.0, 160296.36, "D12345", 0.0, 0.0, 0, 0),
        (2, "CASH_OUT", 215310.3, "C67890", 705.0, 0.0, "D67890", 22425.0, 200.0, 1, 0),
        (3, "TRANSFER", 8009.09, "C54321", 10968.0, 2958.91, "D54321", 0.0, 150.0, 0, 0)
    ]
    
    return spark.createDataFrame(data=data, schema=schema)

def test_rename_dataframe(sample_df):
    map_columns = {
        "step": "paso",
        "type": "tipo",
        "amount": "monto"
    }
    renamed_df = rename_dataframe(sample_df, map_columns)
    
    # comprueba que si esten renombradas las columnas enviadas
    assert "paso" in renamed_df.columns
    assert "tipo" in renamed_df.columns
    assert "monto" in renamed_df.columns
    # Comprueba que el nombre de columna anterior ya no existe
    assert "step" not in renamed_df.columns
    assert "type" not in renamed_df.columns
    assert "amount" not in renamed_df.columns

# FIN Prueba unitaria de la función rename_dataframe

# INICIO Prueba unitaria de la función custom_trim_character

@pytest.fixture
def df_trx(spark):
    schema = StructType([
        StructField("nameOrig", StringType(), True),
        StructField("nameDest", StringType(), True),
        StructField("type", StringType(), True)
    ])
    
    data = [
        ("  C12345@#", "@D12345", "TRANSFER"),
        ("C67890! ", " D67890 ", "CASH_OUT"),
        ("  C54321", "D54321!! ", "TRANSFER")
    ]
    
    return spark.createDataFrame(data=data, schema=schema)

def test_custom_trim_character(df_trx):
    columnas = ["nameOrig", "nameDest"]
    df_limpio = custom_trim_character(df_trx, *columnas)
    
    schema = StructType([
        StructField("nameOrig", StringType(), True),
        StructField("nameDest", StringType(), True),
        StructField("type", StringType(), True)
    ])
    
    expected_data = [
        ("C12345", "D12345", "TRANSFER"),
        ("C67890", "D67890", "CASH_OUT"),
        ("C54321", "D54321", "TRANSFER")
    ]
    expected_df = spark.createDataFrame(data=expected_data, schema=schema)
    
    for columna in columnas:
        assert df_limpio.select(columna).collect() == expected_df.select(columna).collect

# FIN Prueba unitaria de la función custom_trim_character

# INICIO Prueba unitaria de la función apply_column_types

def test_apply_column_types(sample_df):
    column_types = [("amount", "int"), ("step", "string")]
    df_typed = apply_column_types(sample_df, column_types)
    
    assert df_typed.schema["amount"].dataType == IntegerType()
    assert df_typed.schema["step"].dataType == StringType()

# FIN Prueba unitaria de la función apply_column_types

# INICIO Prueba unitaria de la función add_column_tipo_movimiento

def test_add_column_tipo_movimiento(sample_df):
    df_tipo_movimiento = add_column_tipo_movimiento(sample_df)
    expected_values = ["cliente -> comercio", "cliente -> cliente", "comercio -> cliente"]
    result = [row["tipo_movimiento"] for row in df_tipo_movimiento.select("tipo_movimiento").collect()]
    
    assert result == expected_values

# FIN Prueba unitaria de la función add_column_tipo_movimiento

# INICIO Prueba unitaria de la función validate_amount

def test_validate_amount(sample_df):
    df_validated = validate_amount(sample_df)
    expected_values = ["valid", "valid", "valid"]
    result = [row["amount_validated"] for row in df_validated.select("amount_validated").collect()]
    
    assert result == expected_values

#FIN Prueba unitaria de la función validate_amount

# INICIO Prueba unitaria de la función add_tipo_monto_column

def test_add_tipo_monto_column(sample_df):
    df_tipo_monto = add_tipo_monto_column(sample_df)
    expected_values = ["monto_bajo", "monto_alto", "monto_bajo"]
    result = [row["tipo_monto"] for row in df_tipo_monto.select("tipo_monto").collect()]
    
    assert result == expected_values

#FIN Prueba unitaria de la función add_tipo_monto_column