import boto3
from loguru import logger


def load(file, bucket_name, aws_access_key_id, aws_secret_access_key):
    logger.info("Iniciando etapa de Carregamento.")

    file_in_bucket = file.split("\processed")[-1]
    file_in_bucket = file_in_bucket.replace("\\", "/")
    file_in_bucket = file_in_bucket.removeprefix("/")


    # Configurar cliente S3
    try:
        s3 = boto3.client( 
            service_name='s3',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key)
        
        logger.info("Conexão com o S3 bem-sucedida.")
        
        s3.upload_file(file, bucket_name, file_in_bucket)
        logger.info(f"Arquivo '{file_in_bucket}' salvo no bucket '{bucket_name}'")

    except Exception as e:
        logger.error(e)

    logger.info("Finalizando etapa de Carregamento.")
