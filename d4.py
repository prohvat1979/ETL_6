import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://Airflow:1@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()

sql.execute("""drop table if exists spark.d4_1""",con)
sql.execute("""CREATE TABLE if not exists spark.d4_1 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/home/dom/d4_1.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df1.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "d4_1")\
        .mode("append").save()
df2 = df1.toPandas()
# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df2.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df2.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', ax=ax)

sql.execute("""drop table if exists spark.d4_2""",con)
sql.execute("""CREATE TABLE if not exists spark.d4_2 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df3 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/home/dom/d4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df3.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "d4_2")\
        .mode("append").save()
df4 = df3.toPandas()
ax.ticklabel_format(style='plain')
# bar plot
df4.plot(kind='line', 
        x='№', 
        y='долг', 
        color='blue', ax=ax)
df4.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='cyan', ax=ax)

sql.execute("""drop table if exists spark.d4_3""",con)
sql.execute("""CREATE TABLE if not exists spark.d4_3 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df5 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "true")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/home/dom/d4_3.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df5.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "d4_3")\
        .mode("append").save()
df6 = df5.toPandas()
ax.ticklabel_format(style='plain')
# bar plot
df6.plot(kind='line', 
        x='№', 
        y='долг', 
        color='magenta', ax=ax)
df6.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='yellow', ax=ax)

# show the plot
plt.legend(['долг_86689', 'проценты_86689','долг_120000', 'проценты_120000','долг_150000', 'проценты_150000'])
plt.show()

spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))

