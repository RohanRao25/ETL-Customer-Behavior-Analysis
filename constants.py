from pyspark.sql.types import StringType,StructField,StructType,DateType,LongType,FloatType,IntegerType

#This value is being used by the logger file
root_name = "spark_app"

#definition of the schema
data_Schema = StructType([
    StructField("event_time",DateType()),
    StructField("event_type",StringType()),
    StructField("product_id",IntegerType()),
    StructField("category_id",LongType()),
    StructField("category_code",StringType()),
    StructField("brand",StringType()),
    StructField("price",FloatType()),
    StructField("user_id",IntegerType()),
    StructField("user_session",StringType())
                          ]) 


#dateFormat
date_Format = "yyyy-MM-dd HH:mm:ss 'UTC'"

#monthFormat
month_Format = "MMMM"

#updated column name dictionary
column_Name_Dict = {
    "event_time":"dte_event",
    "event_type":"type_event",
    "product_id":"idn_product",
    "category_id":"idn_category",
    "brand" : "nam_brand",
    "price":"price",
    "user_id":"idn_user"
}
