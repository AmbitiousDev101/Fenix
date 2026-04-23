"""
Fenix - Singleton SparkSession Factory
Every Spark module imports get_spark_session() from here.
No other file should ever create a SparkSession.
"""

import os
import sys

# Prevent PySpark workers from crashing on Windows due to encoding issues
os.environ["PYTHONIOENCODING"] = "utf8"
os.environ["PYTHONLEGACYWINDOWSSTDIO"] = "utf8"

if os.name == "nt":
    # Prepend real Python to PATH so workers don't hit the Windows Store shim
    os.environ["PATH"] = sys.prefix + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = "python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
else:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from config.settings import SPARK_CONFIG

_spark_session = None

def get_spark_session() -> SparkSession:
    """Get or create the singleton SparkSession with Iceberg Hadoop catalog config."""
    global _spark_session
    if _spark_session is None or _spark_session._sc._jsc is None:
        builder = SparkSession.builder
        for key, value in SPARK_CONFIG.items():
            builder = builder.config(key, value)
        _spark_session = builder.getOrCreate()
        _spark_session.sparkContext.setLogLevel("ERROR")
    return _spark_session


def stop_spark():
    """Stop the singleton SparkSession and release resources."""
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
