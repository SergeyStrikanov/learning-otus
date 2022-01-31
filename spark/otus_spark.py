'''
Формирование витрины со статистикой криминогенной обстановки в районах Бостона
'''

import sys
import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

PATH_CRIME = sys.argv[1]  # путь к файлу crime.csv
PATH_CODES = sys.argv[2]  # путь к файлу offense_codes.csv
PATH_RESULT = sys.argv[3]  # путь сохранения результата


def main():
    spark = SparkSession.builder.getOrCreate()

    # чтение исходных файлов
    df_crime = spark.read.format("csv").option("header", "true").load(PATH_CRIME)
    df_spr = spark.read.format("csv").option("header", "true").load(PATH_CODES)

    # удаление дубликатов из справочника
    df_spr = df_spr.dropDuplicates(["CODE"])
    # приведение типов
    df_crime = df_crime.withColumn("OFFENSE_CODE", df_crime.OFFENSE_CODE.cast(t.IntegerType()))
    df_crime = df_crime.withColumn("Lat", df_crime.Lat.cast(t.FloatType()))
    df_crime = df_crime.withColumn("Long", df_crime.Long.cast(t.FloatType()))
    df_spr = df_spr.withColumn("CODE", df_spr.CODE.cast(t.IntegerType()))

    # объединение датафреймов
    df_crime = df_crime.join(df_spr, df_crime.OFFENSE_CODE == df_spr.CODE, "left")

    # корректировка имени столбца
    df_crime = df_crime.withColumnRenamed("NAME", "crime_type")

    # сокращение названия нарушений
    df_crime = df_crime.withColumn("crime_type", F.substring_index(df_crime.crime_type, "-", 1))

    # удаление пробелов справа
    df_crime = df_crime.withColumn("crime_type", F.rtrim(df_crime.crime_type))

    # замена NaN и кеширование датафрейма
    df_crime = df_crime.fillna("unknown", subset=["DISTRICT"]).cache()

    # подсчет количества нарушений и медианы по районам
    df_count_median = (
        df_crime
            .groupBy("DISTRICT", "YEAR", "MONTH")
            .agg(
            F.count("YEAR").alias("crimes_total")
        )
    )
    df_count_median = (
        df_count_median
            .groupBy("DISTRICT")
            .agg(
            F.sum("crimes_total").alias("crime_total"),
            F.percentile_approx("crimes_total", 0.5).alias("crimes_monthly")
        )
    )

    # определение средних координат по районам
    df_avg_coor = (
        df_crime
            .groupBy("DISTRICT")
            .agg(
            F.avg("Lat").alias("lat"),
            F.avg("Long").alias("lng")
        )
    )

    # определение трёх самых частых нарушений по убыванию
    df_freq_types = (
        df_crime
            .groupBy("DISTRICT", "crime_type")
            .agg(F.count("OFFENSE_CODE").alias("count"))
    )
    window = Window.partitionBy(df_freq_types['DISTRICT']).orderBy(F.col("count").desc())
    df_freq_types = df_freq_types.select('*', F.row_number().over(window).alias('rank'))
    df_freq_types = df_freq_types.filter(df_freq_types.rank <= 3)
    df_freq_types = df_freq_types.groupBy("DISTRICT").agg(F.collect_list("crime_type").alias("crime_type"))
    df_freq_types = (
        df_freq_types
            .withColumn("frequent_crime_types", F.concat_ws(", ", "crime_type"))
            .select(F.col("DISTRICT"), F.col("frequent_crime_types"))
    )

    # формирование итогового датафрейма
    df_showcase = df_count_median.join(df_freq_types, ["DISTRICT"]).join(df_avg_coor, ["DISTRICT"])

    # сохранение данных
    df_showcase.coalesce(1).write.mode("overwrite").format("parquet").save(PATH_RESULT)

    spark.stop()


main()
