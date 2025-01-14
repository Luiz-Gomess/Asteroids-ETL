import boto3
from datetime import date


def load(filename, bucket_name):
    # Configurar cliente S3
    s3 = boto3.client('s3')

    # Informações do bucket e da pasta
    bucket_name = "asteroidsbucket1"
    folder_name = date.today().strftime("%m") + "/" +  filename 

    # # Criar a pasta
    try:
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"Arquivo '{folder_name}' criado no bucket '{bucket_name}'")
    except Exception as e:
        raise Exception(f"Erro ao criar a pasta: {e}")
