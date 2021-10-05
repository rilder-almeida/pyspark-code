from pyspark.sql.types import *
from pyspark.sql.functions import *
from unidecode import unidecode

@udf(StringType())
def unidecode_udf(string):
    if not string:
        return None
    else:
        return unidecode(string)

cols = ["email","cidade"]

for col in cols:
    df = df.withColumn(col, unidecode_udf(lower(col)))