import boto3
import json 
import logging

logger = logging.Logger(__name__)
def get_secret(secret_name, region_name='us-east-2'):
    """Rertive secrets from AWS secret Manager"""
    session = boto3.session.Session()
    client = session.client(service_name = 'secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

    except Exception as e:
        logger.error(f"Secrets retrieval error {e}")
        raise ValueError(f"Secrets retrieval error {e}")