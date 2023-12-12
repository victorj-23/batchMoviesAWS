from pyspark.sql.functions import *


# Carregando os dados
df = spark.read.parquet("s3://projetc-etl-aws/Trusted/TMDB/2023/10/5/animes/")

# Criando a coluna 'ID'
df = df.withColumn("id", monotonically_increasing_id())

# Filtrando as linhas onde a coluna de array 'pais_original' contém o valor 'JP'
df_filtrado = df.filter(array_contains(col("pais_original"), "JP"))

# Explodindo o array da coluna 'pais_original' para criar uma nova coluna 'pais_origem' para cada valor no array
df_filtrado = df_filtrado.select("*", explode("pais_original").alias("pais_origem"))

# Removendo colunas indesejadas
df_filtrado = df_filtrado.drop("pais_original")
df_filtrado = df_filtrado.drop("visao_geral")

# Filtrando as linhas onde a coluna 'pais_origem' é igual a 'JP'
df_filtrado = df_filtrado.filter(col("pais_origem") == "JP")

# Organizando as colunas do dataframe
df_filtrado = df_filtrado.select("id", "titulo", "votos", "media_de_votos", "popularidade", "data_de_lancamento", "pais_origem")

# Salvando o dataframe resultante
df_filtrado.write.format('parquet').save("s3://projetc-etl-aws/Refined/TMDB/", mode="overwrite")
