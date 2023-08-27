"""
logging
~~~~~~~

This module contains a class that wraps the log4j object instantiated
by the active sparkSession, enabling Log4j logging for PySpark.
"""

from pyspark.sql import SparkSession
from constants import root_name
class Log4j:

    """Wrapper class for Log4j JVM object.

    """

    def __init__(self,spark:SparkSession):

        """
        This constructor method creates a logger object by using the sparkSession 
        receieved while the object creation
        param : self object & SparkSession
        return : None
        """

        log4j = spark._jvm.org.apache.log4j
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_name+"."+app_name)

    def info(self,message):
        """
            This method is used to log an information.
            param : self object & message
            return : None
        """
        self.logger.info(message)



    def warn(self,message):
        """
            This method is used to log a warning.
            param : self object & message
            return : None
        """
        self.logger.warn(message)



    def error(self,message):
        """
            This method is used to log an error.
            param : self object & message
            return : None
        """
        self.logger.error(message)



    def debug(self,message):
        """
            This method is used to log a debug message.
            param : self object & message
            return : None
        """
        self.logger.debug(message)
    
        