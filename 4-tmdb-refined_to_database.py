# Carregando os dados e criando uma view temporária no Spark
df = spark.read.parquet("s3://projetc-etl-aws/Refined/TMDB/")
df.createOrReplaceTempView("temp_anime")

# Criando um banco de dados no S3
spark.sql("CREATE DATABASE IF NOT EXISTS animes_db LOCATION 's3://projetc-etl-aws/DataBase/animes_db'")
spark.sql("USE animes_db")

# Criando a tabela 'anime' utilizando a tabela temporaria 'temp_anime'
spark.sql("CREATE TABLE IF NOT EXISTS anime AS SELECT * FROM temp_anime")

# Criando as dimensoes dentro do DB
spark.sql('CREATE TABLE IF NOT EXISTS dim_origem AS SELECT id, pais_origem FROM animes_db.anime')
spark.sql('CREATE TABLE IF NOT EXISTS dim_tempo AS SELECT id, data_de_lancamento FROM animes_db.anime')
spark.sql('CREATE TABLE IF NOT EXISTS dim_popularidade AS SELECT id, votos, media_de_votos, popularidade FROM animes_db.anime')
spark.sql('CREATE TABLE IF NOT EXISTS fato_anime AS SELECT id, titulo FROM animes_db.anime')
