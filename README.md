# Projeto ETL com Spark e AWS S3

## Descrição do Projeto

Este projeto consiste em um processo ETL (Extract, Transform, Load) utilizando o Apache Spark em conjunto com o AWS S3 para manipulação de dados relacionados a animes, extraídos da API TMDB.

## Scripts e Funcionalidades

### 1. Extração (E)

#### 1.1. Script: `lambda.py`

O script `lambda.py` é responsável por realizar a extração de dados da API do TMDB usando a função Lambda da AWS. Ele requer os dados que serão posteriormente processados e refinados antes de serem armazenados no AWS S3.

### 2. Carregamento (L)

#### 2.1. Script: `tmdb-raw_to_trusted.py`

O script `tmdb-raw_to_trusted.py` executa a carga dos dados brutos (raw) no AWS S3 e os transforma em um formato mais confiável (trusted), utilizando o Apache Spark para a realização dessa transformação.

### 3. Transformação Adicional

#### 3.1. Script: `tmdb-trusted_to_refined.py`

O script `tmdb-trusted_to_refined.py` continua o processo de transformação dos dados, refinindo ainda mais as informações armazenadas no AWS S3 para um formato mais apropriado para análises futuras.

### 4. Manipulação de Dados no Spark

#### 4.1. Script: `tmdb_refined_to_database.py`

O script `tmdb_refined_to_database.py` utiliza Spark SQL para criar um banco de dados, tabelas principais e dimensões, organizando os dados refinados para análises posteriores.

### 5. Execução do Projeto

Para executar o projeto, siga os seguintes passos:

1. Configure as credenciais da AWS no arquivo de configuração.
2. Execute os scripts na seguinte ordem:
    - `lambda.py` para extração inicial de dados.
    - `tmdb-raw_to_trusted.py` para carga e transformação dos dados brutos.
    - `tmdb-trusted_to_refined.py` para continuar a transformação dos dados para um formato refinado.
    - `tmdb_refined_to_database.py` para manipulação final dos dados no Spark e armazenamento no banco de dados.
