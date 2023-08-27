"""
            This file is the entry point of the application. 
            This file contains the main etl Job.
           
"""


from dependencies.spark import Start_Spark
from constants import data_Schema,month_Format,date_Format, column_Name_Dict
from pyspark.sql import dataframe
from pyspark.sql.functions import col, date_format,count



def Drop_Columns(df:dataframe):
    """
            This method is used to drop columns from the data frame.
            param : dataframe
            return : dataframe
    """

    
    return df.drop("category_code","user_session")



def Rename_Columns(df:dataframe):
    """
            This method is used to rename the columns as per the standards
            param : dataframe
            return : dataframe
    """
    for key in column_Name_Dict.keys():
        df = df.withColumnRenamed(key,column_Name_Dict.get(key))

    return df



def Filter_Data(df:dataframe):
    """
            This method is used to filter the dataframe
            param : dataframe
            return : dataframe
    """

    df_without_null_val = df.filter(col("nam_brand").isNotNull())
    filter_df = df_without_null_val.filter((col("type_event") == "remove_from_cart")| (col("type_event") == "purchase") )
    return filter_df



def Add_Column_To_DF(df:dataframe):
    """
            This method is used to add new column to the dataframe
            param : dataframe
            return : dataframe
    """

    return df.withColumn("month_event",date_format(col("dte_event"),month_Format))



def Get_Grouped_Data(df:dataframe):
    """
            This method is used to get the grouped data feom the dataframe
            param : dataframe
            return : dataframe
    """

    return df.groupBy("nam_brand","type_event").agg(count("idn_user").alias("no_of_activities"))



#Extract Method

def Extract_Data(spark):

    """
            This method is used to rxtract data from the data source
            param : None
            return : dataframe
    """
    
    extracted_df = spark.read. \
        format("csv"). \
        option("header", "true"). \
        schema(data_Schema). \
        option("mode","FAILFAST"). \
        option("dateFormat",date_Format). \
        load("data/2019*.csv")
    
    
    return extracted_df


#Transform method

def Transform_Data(df : dataframe,logger) :
    
    """
            This method is used to Transform the dataframe as per the requirement
            param : dataframe
            return : dataframe
    """

    logger.info("Drop all unwanted columns...")
    df_with_dropped_cols = Drop_Columns(df)
    logger.info("Renaming columns as per standard...")
    df_with_dropped_cols = Rename_Columns(df_with_dropped_cols)
    logger.info("Filtering the dataframe for relevant data")
    df_with_filter_data = Filter_Data(df_with_dropped_cols)
    logger.info("Extracting month details from the data and adding it as another columns...")
    updated_df = Add_Column_To_DF(df_with_filter_data)
    logger.info("Creating the final data...")
    grouped_df = Get_Grouped_Data(updated_df)

    return grouped_df
    



#Load method
def Load_Data(df: dataframe):
    """
            This method is used to load the transformed data into the sink
            param : dataframe
            return : None
    """

    df.write.format("csv").mode("overwrite").option("header","true").partitionBy("type_event").option("path","tmp/data/").save()
    

        


#Entry point for the application
def main():

    """
            This method is the entry point for the application
            param : None
            return : None
    """
    #get the spark session and logger object
    spark, logger = Start_Spark()

    logger.warn('Spark is up & running...')
    logger.info("Attemtping data extract operation...")
    df = Extract_Data(spark)
    logger.info("data successfully extracted! Attempting transformation operation...")
    transformed_df = Transform_Data(df,logger)
    logger.info("data tranformation successfully! Attempting to Write the new data...")
    Load_Data(transformed_df)
    
    logger.warn("data has been successfully transferred to sink! Killing spark now...")
    spark.stop()


if __name__ == "__main__":
    main()