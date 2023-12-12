from datetime import date


# Variaveis para facilitar a busca ao arquivo
day_reading = 5 
month_reading = 10
year_reading = 2023
file_name = "animes.json"

# Leitura do arquivo
df = spark.read.json(f"s3://projetc-etl-aws/Raw/TMDB/JSON/Series/{year_reading}/{month_reading}/{day_reading}/{file_name}")

# Variaveis para facilitar o salvamento como parquet
day = date.today().day
month = date.today().month
year = date.today().year
name_directory = "animes"

# Salvando como .parquet
df.write.format('parquet').save(f"s3://projetc-etl-aws/Trusted/TMDB/{year}/{month}/{day}/{name_directory}", mode="overwrite")
