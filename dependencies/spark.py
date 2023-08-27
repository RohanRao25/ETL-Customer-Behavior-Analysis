from pyspark.sql import SparkSession
from dependencies.logger import Log4j
from dependencies.helpers import Read_Spark_Conf_File

def Start_Spark():
    """
            This method is used to start the spark application 
            param : None
            return : SparkSession & Logger object
    """

    spark_conf = Read_Spark_Conf_File()

    spark_session = SparkSession.builder.config(conf = spark_conf).getOrCreate()

    logger = Log4j(spark_session)

    return spark_session, logger


