# import boto3
# from datetime import date


# def send_to_s3(file):
#     # Configurar cliente S3
#     s3 = boto3.client('s3')

#     # Informações do bucket e da pasta
#     bucket_name = "asteroidsbucket1"
#     folder_name = date.today().strftime("%m") + "/" +  file 

#     # # Criar a pasta
#     try:
#         s3.put_object(Bucket=bucket_name, Key=folder_name)
#         print(f"Pasta '{folder_name}' criada no bucket '{bucket_name}'")
#     except Exception as e:
#         raise Exception(f"Erro ao criar a pasta: {e}")
