from pyspark import SparkConf
import configparser

def Read_Spark_Conf_File():

    """
        This method is being used in the spark.py file -> Start_Spark() method.
        This method read the configs from the spark.con file and returns a SparkConf object.
        params: None
        Return - SparkConf object
    """
    
    config_parser = configparser.ConfigParser()
    config_parser.read("spark.conf")
    spark_conf = SparkConf()

    for key,val in config_parser.items("spark_app_config"):
        spark_conf.set(key,val)

    return spark_conf



