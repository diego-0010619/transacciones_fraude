from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, trim, col, lit, when, unix_timestamp, concat

""" Transformaciones producto transacciones fraude """

# 1. Función para renombrar columnas
def rename_dataframe(df: DataFrame, map_columns: dict) -> DataFrame:
    """
    Retorna un DataFrame con columnas renombradas de acuerdo a map_columns.
    
    Args:
        df (DataFrame): DataFrame de entrada para renombrar
        map_columns (dict): Diccionario con el mapeo de columnas
    
    Returns:
        DataFrame: DataFrame de PySpark con datos transformados
    """
    existing_columns = df.columns

    for old_column, new_column in map_columns.items():
        # Renombrar solo si la columna existe en el DataFrame
        if old_column in existing_columns:
            df = df.withColumnRenamed(old_column, new_column)
    
    return df

# 2. Funcion para limpiar un campo enviado en el parámetro, trim del campo enviado.
def custom_trim_character(df: DataFrame, *columnas: str) -> DataFrame:
    """
    Return a dataframe with specified columns cleaned by trimming the specified character.
    Args:
        df (DataFrame): Input dataframe to apply transformation.
        columnas (list): List of column names to apply the trim function.
    Returns:
        DataFrame: Spark's DataFrame with transformed data.
    """
    for columna in columnas:
        df = df.withColumn(columna, regexp_replace(col(columna), r'[^a-zA-Z0-9\s]', ''))
        df = df.withColumn(columna, trim(col(columna)))
    return df

# 3. Función para tipar el tipo de dato de cada columna
def apply_column_types(df: DataFrame, column_types: list) -> DataFrame:
    """
    Transforma el tipo de dato de cada columna en el DataFrame según lo especificado en column_types.
    
    Args:
        df (DataFrame): El DataFrame de entrada de PySpark.
        column_types (list): Lista de tuplas donde cada tupla contiene el nombre de la columna y su tipo de dato.
    
    Returns:
        DataFrame: DataFrame con los tipos de datos aplicados.
    """
    for column_name, column_type in column_types:
        if column_name in df.columns:
            df = df.withColumn(column_name, df[column_name].cast(column_type))
    
    return df

# 4. Función para agregar columna tipo_movimiento; esta identifica el tipo de usuario que esta transando usando el número de cuenta(name_org, name_dest).
def add_column_tipo_movimiento(df: DataFrame) -> DataFrame:
    # Usamos la función `when` para evaluar las condiciones y crear la columna `tipo_movimiento`
    df = df.withColumn(
        "tipo_movimiento",
        when(
            (col("name_orig").startswith("C")) & (col("name_dest").startswith("M")), "cliente -> comercio"
        ).when(
            (col("name_orig").startswith("C")) & (col("name_dest").startswith("C")), "cliente -> cliente"
        ).when(
            (col("name_orig").startswith("M")) & (col("name_dest").startswith("C")), "comercio -> cliente"
        ).when(
            (col("name_orig").startswith("M")) & (col("name_dest").startswith("M")), "comercio -> comercio"
        ).otherwise("otro")
    )
    return df

# 5. Función para validar el valor de la transacción, montos negativos.
def validate_amount(df: DataFrame) -> DataFrame:
    """
    Validates the 'amount' column to check if the value is greater than or equal to 0.
    If the value is less than 0, creates a new column 'amount_validated' with the value 'invalid'.
    Otherwise, 'amount_validated' will have the value 'valid'.
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        DataFrame: DataFrame with the new 'amount_validated' column
    """
    df = df.withColumn(
        "amount_validated", 
        when(col("amount") >= 0, "valid").otherwise("invalid")
    )
    
    return df

# 6. Adicionar columna para validar el monto de la transacción.
def add_tipo_monto_column(df: DataFrame) -> DataFrame:
    """
    Adds a new column 'tipo_monto' to the DataFrame based on the value of 'amount'.
    - 'monto_bajo' if amount <= 200,000
    - 'monto_alto' if amount > 200,000
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        DataFrame: DataFrame with the new 'tipo_monto' column
    """
    df = df.withColumn(
        "tipo_monto", 
        when((col("amount") >= 0) & (col("amount") <= 200000), "monto_bajo")
        .when(col("amount") > 200000, "monto_alto")
        .otherwise("monto_invalido")
    )
    
    return df

# 7. Add field execution_date
def add_field_execution_date(df: DataFrame, execution_date: str) -> DataFrame:
    """
    Adds a new column 'execution_date' with the provided date to the DataFrame
    Args:
        df (DataFrame): Input DataFrame
        execution_date (str): Name of the column to be added
        date_value (str): The date value to be added to the new column
    Returns:
        DataFrame: Spark's DataFrame with the new column 'execution_date'
    """
    # Agregar la columna 'execution_date'
    df = df.withColumn("execution_date", lit(execution_date))

    return df

# 8. Convirtiendo columna execution_date en formato Unix, para crear la llave precombinada
def convert_column_comb_format_unix(df: DataFrame) -> DataFrame:
    """
        Returns a dataframe add column 'execution_date_unix' format unix
        Args:
            df (DataFrame): Input df to apply tf
        Returns:
            DataFrame: Spark's Dataframe with transformed data
    """
    df = df.withColumn("execution_date_unix", unix_timestamp(df["execution_date"], "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("primary_key", concat(
            df["step"], lit("_"),
            df["execution_date_unix"], lit("_"),
            df["name_orig"], lit("_"),
            df["name_dest"], lit("_"),
            col("amount").cast("int")))

    return df