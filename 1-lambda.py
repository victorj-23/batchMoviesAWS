import json
import boto3
import requests
from datetime import date
from time import sleep


def lambda_handler(event, context):
    # Defininindo as informações de acesso
    AWS_ACCESS_KEY_ID = '<KEY>'
    AWS_SECRET_ACCESS_KEY = '<KEY SECRET>'
    REGION = 'us-east-1'

    # Criando um objeto do cliente do S3
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=REGION)

    # Variáveis complementares a URL de requisição
    api_key = "<KEY>"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    series = []

    # Loop para fazer a requisição de várias páginas
    for page in range(1, 501):
        url = f"https://api.themoviedb.org/3/discover/tv?include_adult=false&include_null_first_air_dates=false&language=en-US&page={page}&sort_by=vote_count.desc&with_genres=16&with_keywords=anime"
        response = requests.get(url, headers=headers)
        data = response.json()
        try:
            for serie in data['results']:
                df = {'Titulo': serie['name'],
                      'Data de lançamento': serie['first_air_date'],
                      'Visão geral': serie['overview'],
                      'Votos': serie['vote_count'],
                      'Média de votos:': serie['vote_average'],
                      'Popularidade:': serie['popularity'],
                      'País original:': serie['origin_country']}
                series.append(df)
            
            # Pausa a cada 50 paginas para evitar bloqueio da API
            if page % 50 == 0:
                sleep(1.5)

        except KeyError:
            continue

    # Variáveis para o redirecionamento do diretório
    bucket = 'projetc-etl-aws'
    standard_storage = 'Raw/TMDB/JSON'
    name_file = 'animes.json'
    directory = f'{standard_storage}/Series/{date.today().year}/{date.today().month}/{date.today().day}/{name_file}'

    # Transformando a lista em objeto JSON
    json_file = json.dumps(series, ensure_ascii=False).encode('utf-8')

    # Carregando o arquivo para o bucket
    s3.put_object(
        Key=directory,
        Body=json_file,
        Bucket=bucket)
