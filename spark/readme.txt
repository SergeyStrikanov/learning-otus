Скрипт формирует витрину данных (file.parquet) из двух файлов (crime.csv, offense_codes.csv).
В результирующем файле хранится статистика криминогенной обстановки в районах Бостона.
Скрипт принимает 3 обязательных аргумента:
  1. путь к файлу crime.csv
  2. путь к файлу offense_codes.csv
  3. путь сохранения результата
  
Пример:
  spark-submit otus_spark.py /home/user/path/input_folder/crime.csv /home/user/path/input_folder/offense_codes.csv /home/user/path/output_folderс
