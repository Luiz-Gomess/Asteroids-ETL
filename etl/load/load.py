import boto3
from loguru import logger


def load(file, bucket_name, aws_access_key_id, aws_secret_access_key):
    """Etapa final de Carregamento

    Args:
        file (str): Caminho para o arquivo a ser carregado.
        bucket_name (str): Nome do bucket no s3.
        aws_access_key_id (str): ID da chave de acesso de uma conta AWS.
        aws_secret_access_key (str): Conteúdo da chave de acesso de uma conta da AWS.
    """
    logger.info("Etapa de Carregamento inicializada.")

    #Formatando o nome do arquivo para o bucket
    file_in_bucket = file.split("\processed")[-1]
    file_in_bucket = file_in_bucket.replace("\\", "/")
    file_in_bucket = file_in_bucket.removeprefix("/")


    try:
        # Configura o cliente S3
        s3 = boto3.client( 
            service_name='s3',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key)
        
        logger.info("Conexão com o S3 bem-sucedida.")
        
        # Carrega o arquivo
        s3.upload_file(file, bucket_name, file_in_bucket)
        logger.info(f"Arquivo '{file_in_bucket}' salvo no bucket '{bucket_name}'")

    except Exception as e:
        logger.error(e)

    logger.info("Etapa de Carregamento finalizada.")
