"""SimpleApp.py"""
from pyspark.sql import SparkSession
import sys

LOAD_DT=sys.argv[1]

spark = SparkSession.builder.appName('Simple').getOrCreate()


df1 = spark.read.parquet(f"/home/manggee/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("one_day")

df2 = spark.sql(f"""
select sum(salesAmt) as SA, sum(audiCnt) as AC, sum(showCnt) as SC, multiMovieYn, '{LOAD_DT}' AS load_dt
from one_day
group by multiMovieYn
""")

df2.createOrReplaceTempView("multiSUM")

df3 = spark.sql(f"""
select sum(salesAmt) as SA, sum(audiCnt) as AC, sum(showCnt) as SC, repNationCd
, '{LOAD_DT}' AS load_dt
from one_day
group by repNationCd
""")

df3.createOrReplaceTempView("repSUM")



df2.write.mode('overwrite').partitionBy("load_dt").parquet("/home/manggee/data/movie/sum-multi")
df3.write.mode('overwrite').partitionBy("load_dt").parquet("/home/manggee/data/movie/sum-nation")
spark.stop()

