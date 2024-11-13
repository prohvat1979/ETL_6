/*
export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/dom/d3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.date_format

val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/home/dom/d3.xlsx")
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s3_1")
        .mode("overwrite").save()
		
		val q = """
		SELECT ID_Тикет, 
		FROM_UNIXTIME(Status_Time) Status_Time, 
		(LEAD(Status_Time) OVER(PARTITION BY ID_Тикет ORDER BY Status_Time)-Status_Time)/3600 Длительность,
		CASE WHEN Статус IS NULL THEN @PREV1 ELSE @PREV1:=Статус END Статус,
		CASE WHEN Группа IS NULL THEN @PREV2 ELSE @PREV2:=Группа END Группа,
		Назначение
		FROM (
			SELECT ID_Тикет, Status_Time, Статус, IF(ROW_NUMBER() OVER(PARTITION BY ID_Тикет ORDER BY Status_Time)=1 AND Назначение IS NULL,'',Группа) Группа, Назначение
			FROM (
				SELECT DISTINCT a.objectid ID_Тикет, a.restime Status_Time, Статус, Группа, Назначение,(SELECT @PREV1:=''),(SELECT @PREV2:='')
				FROM (
					SELECT DISTINCT objectid, restime 
					FROM spark.s3_1
					WHERE fieldname IN ('gname2','status')
				) a
				LEFT JOIN (
					SELECT DISTINCT objectid, restime, fieldvalue Статус
					FROM spark.s3_1
					WHERE fieldname IN ('status')
				) a1 ON a.objectid=a1.objectid AND a.restime=a1.restime
				LEFT JOIN (
					SELECT DISTINCT objectid, restime, fieldvalue Группа, 1 Назначение
					FROM spark.s3_1
					WHERE fieldname IN ('gname2')
				) a2 ON a.objectid=a2.objectid AND a.restime=a2.restime
			) b1
		) b2
		"""
		spark.read.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q)
		.load()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s3_2")
        .mode("overwrite").save()
		
var df2 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s3_2")
		.load()
		df2.select(col("ID_Тикет"),date_format(col("Status_Time"),"dd.MM.yyyy hh.mm") as "Status_Time",col("Группа"),col("Статус"))
		.withColumn("Статус"
			,when(col("Статус") === lit("Зарегистрирован"), "З").otherwise(
				when(col("Статус") === lit("Назначен"), "Н").otherwise(
				when(col("Статус") === lit("В работе"), "ВР").otherwise(
				when(col("Статус") === lit("Решен"), "Р").otherwise(
				when(col("Статус") === lit("Исследование ситуации"), "ИС").otherwise(
				when(col("Статус") === lit("Закрыт"), "ЗТ").otherwise(col("Статус")))))))
		)		
		.withColumn("Назначение", concat($"Status_Time", lit(" | "), $"Статус", lit(" | "), $"Группа")) 
		.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s3_3")
        .mode("overwrite").save()	
		
		val qq = """
		SELECT m1.`ID_Тикет`,GROUP_CONCAT(m2.Status_Time,' | ',m2.`Статус`,' | ',m2.`Группа` ORDER BY m2.Status_Time SEPARATOR ' || ') AS `Статус`
		FROM spark.s3_3 m1
		JOIN (
			SELECT `ID_Тикет`,Status_Time,`Статус`,`Группа`
			FROM spark.s3_3
		) m2 ON m1.`ID_Тикет` = m2.`ID_Тикет`
		GROUP BY m1.`ID_Тикет`
		"""
var df3 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", qq)
		.load()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "s3_4")
        .mode("overwrite").save()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)