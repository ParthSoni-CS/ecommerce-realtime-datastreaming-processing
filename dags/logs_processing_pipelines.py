from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
import json 
from elasticsearch.helpers import bulk
import re 
import logging
import boto3
# from .utils import get_secrets

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
    
def parse_log_entry(log_entry):
    log_pattern = r'(?P<ip>[\d\.]+) -- \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) HTTP/(?P<http_version>\d\.\d)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" (?P<user_agent>.*)'

    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Invalid log format: {log_entry}")  # Print the failing log entry for debugging
        return None

    data = match.groupdict()

    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], "%b %d %Y, %H:%M:%S")
        data['@timestamp'] = parsed_timestamp.isoformat()
    
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None

    return data


def consume_and_index_logs():
    secrets = get_secret("MWAA_SECRETS_V2")
    consumer_config = {
        'bootstrap.servers': secrets['KAFKA_BOOSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }
    es_config = {
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key': secrets['ELASTICSEARCH_API_KEY']
    }
    consumer = Consumer(consumer_config)
    print(consumer)
    es = Elasticsearch(**es_config)
    topic = 'billion_website_logs'
    consumer.subscribe([topic])

    try:
        index_name = 'billion_website_logs'
        # print("Check if name is created or not")
        if not es.indices.exists(index = index_name):
            es.indices.create(index = index_name)
            logger.info(f'Created index : {index_name}')
    
    except Exception as e:
        logger.error(f'Failed to create index : {index_name} {e}')
    
    try:
        logs = []
        while True:
            msg = consumer.poll(timeout = 1.0)
            print("Message from consumer : ", msg)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
        
            log_entry = msg.value().decode('utf-8')
            parsed_log = parse_log_entry(log_entry)
            print("Parsed logs : ", parsed_log)
            if parsed_log:
                logs.append(parsed_log)
            
            # index when 15000 logs are collected
            if len(logs)>=5:
                actions = [
                    {
                        '_op_time': 'create',
                        '_index': index_name,
                        '_source': log
                    }
                    for log in logs
                ]
                print("actions : ", actions)
                success, failed = bulk(es, actions, refresh = True)
                logger.info(f'Indexed {success} logs, {len(failed)} failed')
                logs = []

    except Exception as e:
        logger.error(f'Failed to index log : {e}')

    try:
            # index any remaining logs
        if logs:
            actions = [
                {
                    '_op_time': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs
            ]
            success, failed = bulk(es, actions, refresh = True)
    
    except Exception as e:
        logger.error(f'Log Processing error: {e}')
    
    finally:
        consumer.close()
        es.close()
    

default_args = {
    'owner':'Dr Neuro',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retries_delay': timedelta(seconds = 5)
}

dag = DAG(
    'log_consumer_pipeline',
    default_args = default_args,
    description='Consume and Index synthetic logs',
    schedule_interval='*/5 * * * *',
    start_date=datetime(year = 2025, month = 2, day = 8),
    catchup=False,
    tags = ['logs', 'kafka', 'production']
)

consume_logs_task = PythonOperator(
    task_id = 'generate_and_consume_logs', 
    python_callable=consume_and_index_logs,
    dag = dag
)

# consume_and_index_logs()