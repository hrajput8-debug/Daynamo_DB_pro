import boto3
from botocore.exceptions import ClientError

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
import json


def getDynamoDBConnection(config=None, endpoint=None, port=None, local=False, use_instance_metadata=False):
    session_kwargs = {}
    resource_kwargs = {}

    # If region in config file
    if config is not None:
        if config.has_option('dynamodb', 'region'):
            session_kwargs['region_name'] = config.get('dynamodb', 'region')

    # Command-line endpoint override
    if endpoint is not None:
        resource_kwargs['endpoint_url'] = f"https://{endpoint}"

    # Use EC2 metadata to get region
    if 'region_name' not in session_kwargs and use_instance_metadata:
        response = urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document').read()
        doc = json.loads(response)
        session_kwargs['region_name'] = doc['region']

    # Default region if none set
    if 'region_name' not in session_kwargs:
        session_kwargs['region_name'] = 'us-east-1'

    dynamodb = boto3.resource('dynamodb', **resource_kwargs, **session_kwargs)
    return dynamodb


def createGamesTable(db):
    try:
        table = db.create_table(
            TableName="Games",
            KeySchema=[
                {"AttributeName": "GameId", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "GameId", "AttributeType": "S"},
                {"AttributeName": "HostId", "AttributeType": "S"},
                {"AttributeName": "StatusDate", "AttributeType": "S"},
                {"AttributeName": "OpponentId", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "HostId-StatusDate-index",
                    "KeySchema": [
                        {"AttributeName": "HostId", "KeyType": "HASH"},
                        {"AttributeName": "StatusDate", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
                },
                {
                    "IndexName": "OpponentId-StatusDate-index",
                    "KeySchema": [
                        {"AttributeName": "OpponentId", "KeyType": "HASH"},
                        {"AttributeName": "StatusDate", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )

        table.wait_until_exists()
        print("✅ Games table created successfully.")

    except ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print("ℹ️ Games table already exists.")
            table = db.Table("Games")
        else:
            raise e

    return table


