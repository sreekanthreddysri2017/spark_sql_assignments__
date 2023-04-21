from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from SQL_2.core2.utils_sql2 import *

#creating a spark session
spark=spark_creation()

#creating a dataFrame
df=create_dataFrame(spark)

#creating pivot table
pivot_table=func_pivot(df)

#creating an unpivot table
unpivot_table=func_unpivot(pivot_table)