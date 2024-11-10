import logging
import os
from datetime import datetime

def create_custom_logger(app_name: str):
    """Funcion auxiliar que crea un logger desde el modulo de utilidades utils.py
 
    Args:
        app_name (str):
            Nombre de la aplicacion, en el archivo en que se crea este objeto
            se pasa como argumento > __name__.
 
        https://docs.python.org/3/library/logging.html#logging.LogRecord
        - fecha de ejecucion >   %(asctime)s
        - nivel de mensaje   >   [%(levelname)s]
        - nombre del logger  >   [Logger: %(name)s]
        - ruta al archivo    >   [File: %(pathname)s]
        - mensaje a loguear  >   %(message)s
        - linea de ejecucion >   (Line: %(lineno)d)
 
    Returns:
        logger: Formato de logueo de mensajes
    """
    logger = logging.getLogger(app_name)  # __name__
    MSG_FORMAT = "%(asctime)s [%(levelname)s] [Logger: %(name)s]: %(message)s (Line: %(lineno)d)"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=MSG_FORMAT,
                        datefmt=DATETIME_FORMAT, level=logging.INFO)
    return logger

logger = create_custom_logger(__name__)


